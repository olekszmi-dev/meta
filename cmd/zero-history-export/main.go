package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"

	"go.mau.fi/mautrix-meta/pkg/connector"
	"go.mau.fi/mautrix-meta/pkg/externalhistory"
	"go.mau.fi/mautrix-meta/pkg/igconnector"
)

type options struct {
	provider   string
	nativeURL  string
	synapseURL string
	loginID    string
	outputPath string
	pageSize   int
	apply      bool
}

func env(name string) string {
	return strings.TrimSpace(os.Getenv(name))
}

func envInt(name string, fallback int) int {
	value, err := strconv.Atoi(env(name))
	if err != nil || value == 0 {
		return fallback
	}
	return value
}

func parseOptions() options {
	var value options
	flag.StringVar(&value.provider, "provider", env("ZERO_OFFLINE_HISTORY_PROVIDER"), "messenger or instagram")
	flag.StringVar(&value.nativeURL, "native-database-url", env("ZERO_OFFLINE_HISTORY_NATIVE_DATABASE_URL"), "read-only Mautrix PostgreSQL URL")
	flag.StringVar(&value.synapseURL, "synapse-database-url", env("ZERO_OFFLINE_HISTORY_SYNAPSE_DATABASE_URL"), "read-only Synapse PostgreSQL URL")
	flag.StringVar(&value.loginID, "native-login-id", env("ZERO_OFFLINE_HISTORY_NATIVE_LOGIN_ID"), "exact native user_login ID")
	flag.StringVar(&value.outputPath, "output", env("ZERO_OFFLINE_HISTORY_OUTPUT_PATH"), "protected output bundle path")
	flag.IntVar(&value.pageSize, "page-size", envInt("ZERO_OFFLINE_HISTORY_PAGE_SIZE", externalhistory.DefaultPageSize), "messages per history page")
	flag.BoolVar(&value.apply, "apply", false, "write the protected bundle after successful inspection")
	flag.Parse()
	value.provider = strings.ToLower(strings.TrimSpace(value.provider))
	return value
}

func classifier(provider string) externalhistory.OfflinePortalClassifier {
	switch provider {
	case "messenger":
		return connector.ClassifyOfflineHistoryPortal
	case "instagram":
		return igconnector.ClassifyOfflineHistoryPortal
	default:
		return nil
	}
}

func openPostgres(ctx context.Context, databaseURL string) (*sql.DB, error) {
	database, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, err
	}
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(0)
	database.SetConnMaxLifetime(5 * time.Minute)
	if err = database.PingContext(ctx); err != nil {
		database.Close()
		return nil, err
	}
	return database, nil
}

func run(ctx context.Context, value options) (externalhistory.OfflineReport, error) {
	classify := classifier(value.provider)
	if classify == nil {
		return externalhistory.OfflineReport{}, externalhistory.ErrOfflineProvider
	}
	native, err := openPostgres(ctx, value.nativeURL)
	if err != nil {
		return externalhistory.OfflineReport{}, externalhistory.ErrOfflineNativeRead
	}
	defer native.Close()
	synapse, err := openPostgres(ctx, value.synapseURL)
	if err != nil {
		return externalhistory.OfflineReport{}, externalhistory.ErrOfflineSynapseRead
	}
	defer synapse.Close()
	bundle, report, err := externalhistory.BuildOfflineBundle(ctx, externalhistory.NewPostgresSnapshotReader(native, synapse), externalhistory.OfflineOptions{
		Provider: value.provider, NativeURL: value.nativeURL, SynapseURL: value.synapseURL,
		LoginID: value.loginID, OutputPath: value.outputPath, PageSize: value.pageSize, Classify: classify,
	})
	if err != nil {
		return externalhistory.OfflineReport{}, err
	}
	if value.apply {
		if err = externalhistory.WriteBundleAtomic(value.outputPath, bundle); err != nil {
			return externalhistory.OfflineReport{}, err
		}
	}
	return report, nil
}

func main() {
	value := parseOptions()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	report, err := run(ctx, value)
	if err != nil {
		encoded, _ := json.Marshal(map[string]any{"ok": false, "error": externalhistory.OfflineErrorCode(err)})
		fmt.Fprintln(os.Stderr, string(encoded))
		os.Exit(1)
	}
	encoded, _ := json.Marshal(map[string]any{
		"ok":     true,
		"mode":   map[bool]string{true: "apply", false: "inspect"}[value.apply],
		"report": report,
	})
	fmt.Println(string(encoded))
}

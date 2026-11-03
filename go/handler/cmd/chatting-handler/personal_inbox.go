package main

import (
	"errors"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/bbmb"
	handlerconfig "github.com/EdwardSalkeld/chatting/go/handler/internal/config"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/jmap"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/personalinbox"
)

func newPersonalInbox(config handlerconfig.Config, broker *bbmb.Adapter) (*personalinbox.Service, error) {
	if config.PersonalInbox == nil || !config.PersonalInbox.Enabled {
		return nil, nil
	}
	// Already validated by personalinbox.Decode when the config was loaded.
	personal := *config.PersonalInbox
	mainPath, _ := filepath.Abs(config.DBPath)
	personalPath, _ := filepath.Abs(personal.DBPath)
	if mainPath == personalPath {
		return nil, errors.New("personal inbox must use a separate database")
	}
	token := os.Getenv(personal.TokenEnv)
	if token == "" {
		return nil, errors.New("personal inbox JMAP token is missing")
	}
	telegram := os.Getenv(config.TelegramBotTokenEnv)
	if !personal.DryRun && telegram == "" {
		return nil, errors.New("personal inbox live mode requires the Telegram bot token")
	}
	// Ticketing is the one part that can be left out while the rest runs. A
	// configured team with no key in the environment turns it off rather than
	// failing startup, so action mail is still notified and filed.
	linearKey := ""
	if personal.CreatesTickets() {
		linearKey = os.Getenv(personal.LinearKeyEnv)
		if linearKey == "" {
			log.Printf("personal_inbox_linear_disabled: %s is unset", personal.LinearKeyEnv)
			personal.LinearTeam = ""
		}
	}
	mail := &jmap.Client{
		HTTP:       &http.Client{Timeout: 60 * time.Second},
		Token:      token,
		SessionURL: personal.SessionURL,
	}
	// Resolve remote folders in the inbox cycle. A mail-server outage must not
	// prevent the independent webhook, Telegram and egress servers from starting.
	resolver := &personalinbox.Resolver{Mail: mail}
	store, err := personalinbox.Open(personalPath)
	if err != nil {
		return nil, err
	}
	service := &personalinbox.Service{Config: personal, Store: store, Broker: broker,
		Mail: mail, Folders: resolver, Now: time.Now}
	service.Performer = &personalinbox.HandlerPerformer{Config: personal, Mail: mail, Folders: resolver,
		HTTP:          &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		TelegramToken: telegram, TelegramBase: config.TelegramAPIBaseURL,
		LinearKey: linearKey, LinearURL: "https://api.linear.app/graphql"}
	return service, nil
}

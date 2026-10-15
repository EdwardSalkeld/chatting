package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/bbmb"
	handlerconfig "github.com/EdwardSalkeld/chatting/go/handler/internal/config"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/imap"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/personalinbox"
)

func newPersonalInbox(config handlerconfig.Config, broker *bbmb.Adapter) (*personalinbox.Service, error) {
	if config.PersonalInbox == nil || !config.PersonalInbox.Enabled {
		return nil, nil
	}
	personal := *config.PersonalInbox
	if err := personal.Validate(); err != nil {
		return nil, err
	}
	mainPath, _ := filepath.Abs(config.DBPath)
	personalPath, _ := filepath.Abs(personal.DBPath)
	if mainPath == personalPath {
		return nil, errors.New("personal inbox must use a separate database")
	}
	password := os.Getenv(personal.PasswordEnv)
	if password == "" {
		return nil, errors.New("personal inbox IMAP password is missing")
	}
	token := os.Getenv(config.TelegramBotTokenEnv)
	linearKey := os.Getenv(personal.LinearKeyEnv)
	if !personal.DryRun && (token == "" || linearKey == "") {
		return nil, errors.New("personal inbox live mode requires Telegram and Linear credentials")
	}
	store, err := personalinbox.Open(personalPath)
	if err != nil {
		return nil, err
	}
	dial := func(ctx context.Context) (imap.UIDClient, error) {
		client, err := imap.DialUID(ctx, imap.ClientConfig{Host: personal.Host, Port: personal.Port, UseSSL: true, Timeout: 30 * time.Second})
		if err != nil {
			return nil, err
		}
		if err := client.Login(personal.Username, password); err != nil {
			_ = client.Logout()
			return nil, errors.New("personal inbox login failed")
		}
		return client, nil
	}
	service := &personalinbox.Service{Config: personal, Store: store, Broker: broker, Dial: dial, Now: time.Now}
	service.Performer = &personalinbox.HandlerPerformer{Config: personal, Store: store, Dial: dial,
		HTTP: &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, TelegramToken: token, TelegramBase: config.TelegramAPIBaseURL,
		LinearKey: linearKey, LinearURL: "https://api.linear.app/graphql"}
	return service, nil
}

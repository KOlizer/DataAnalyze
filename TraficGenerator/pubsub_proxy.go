package main

import (
  "bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.kakaoenterprise.in/cloud-platform/kc-pub-sub-sdk-go"
	"github.kakaoenterprise.in/cloud-platform/kc-pub-sub-sdk-go/option"
)

type PubSubRequest struct {
	Messages []struct {
		Data       string            `json:"data"`
		Attributes map[string]string `json:"attributes"`
	} `json:"messages"`
}

type PubSubResponse struct {
	MessageIDs []string `json:"message_ids"`
}

var (
	// 프록시 서버에서 사용하는 Credential 정보는
	// 환경 변수나 플래그로 설정하거나, 간단히 상수로 지정해도 됨.
	credentialID     = "ad5a9ef37e18454dbfb1110ad34d07da"
	credentialSecret = "8d6d9b673e7d9c8a5c5c3499a0c3a920bc2a246dbe3fd893f282a7ee25fe005f796062"
)

func forwardToALB(req PubSubRequest) error {
	// ALB가 로그를 받는 REST API 엔드포인트 지정
	albURL := "http://210.109.58.20/"
	
	// ALB에 전송할 payload 생성 (필요한 형식에 맞게 조정)
	payload, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("payload marshal error: %v", err)
	}
	
	// POST 요청 전송
	resp, err := http.Post(albURL, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("ALB POST error: %v", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("ALB response status: %d", resp.StatusCode)
	}
	return nil
}

func publishHandler(w http.ResponseWriter, r *http.Request) {
	// URL 예: /v1/domains/{domainID}/projects/{projectID}/topics/{topicName}/publish
	// URL 경로를 분해하여 필요한 정보를 추출
	pathParts := strings.Split(r.URL.Path, "/")
	if len(pathParts) < 8 {
		http.Error(w, "Invalid URL", http.StatusBadRequest)
		return
	}
	domainID := pathParts[3]
	projectID := pathParts[5]
	topicName := pathParts[7]

	var req PubSubRequest
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("JSON decode error: %v", err), http.StatusBadRequest)
		return
	}

	if len(req.Messages) == 0 {
		http.Error(w, "No messages provided", http.StatusBadRequest)
		return
	}

	// Setup AccessKey 및 SDK 옵션
	accessKey := pubsub.AccessKey{
		CredentialID:     credentialID,
		CredentialSecret: credentialSecret,
	}
	opts := []option.ClientOption{
		option.WithAccessKey(accessKey),
		// 기본 엔드포인트는 내부이므로 별도 지정 없이 진행가능
	}

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, domainID, projectID, opts...)
	if err != nil {
		http.Error(w, fmt.Sprintf("pubsub.NewClient error: %v", err), http.StatusInternalServerError)
		return
	}
	defer client.Close()

	topic := client.Topic(topicName)
	var messageIDs []string

	for _, m := range req.Messages {
		// SDK는 Base64 인코딩된 데이터를 요구하므로, 한 번 디코딩 후 다시 인코딩할 수 있음
		// (또는 그대로 전달하여도 무방한지 확인)
		decoded, err := base64.StdEncoding.DecodeString(m.Data)
		if err != nil {
			http.Error(w, fmt.Sprintf("Base64 decode error: %v", err), http.StatusBadRequest)
			return
		}
		encodedData := base64.StdEncoding.EncodeToString(decoded)

		msg := &pubsub.Message{
			Data:       encodedData,
			Attributes: m.Attributes,
		}

		result := topic.Publish(ctx, msg)
		msgID, err := result.Get(ctx)
		if err != nil {
			http.Error(w, fmt.Sprintf("Publish error: %v", err), http.StatusInternalServerError)
			return
		}
		messageIDs = append(messageIDs, fmt.Sprintf("%v", msgID))
	}

	resp := PubSubResponse{MessageIDs: messageIDs}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func main() {
	http.HandleFunc("/v1/domains/", publishHandler)
	log.Println("Starting Pub/Sub proxy on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

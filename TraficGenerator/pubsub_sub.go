package main

import (
    "context"
    "encoding/base64"
    "fmt"
    "log"
    "time"

    "github.kakaoenterprise.in/cloud-platform/kc-pub-sub-sdk-go"
    "github.kakaoenterprise.in/cloud-platform/kc-pub-sub-sdk-go/option"
)

func main() {
    // 전체 수신 작업에 대한 시간 제한 설정 (예: 5분)
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
    defer cancel()

    // 카카오클라우드 콘솔에서 발급받은 환경 정보
    domainID := "fa22d0db818f48829cf8b7849e3a0a26"
    projectID := "86099dec56044a43ac3f92a40784929b"
    subscriptionName := "PullSuscription" // 구독 이름 설정
    credentialID := "618260d9181d4d1ead8b4b70a8608ec4"
    credentialSecret := "846b5e041cdba312829f167a4719721b74580192b42aa0e29dc39fb46e7aaaece5273e"

    // AccessKey 구성
    accessKey := pubsub.AccessKey{
        CredentialID:     credentialID,
        CredentialSecret: credentialSecret,
    }

    // Client 옵션 구성
    opts := []option.ClientOption{
        option.WithAccessKey(accessKey),
    }

    // Pub/Sub 클라이언트 생성
    client, err := pubsub.NewClient(ctx, domainID, projectID, opts...)
    if err != nil {
        log.Fatalf("pubsub.NewClient error: %v", err)
    }
    defer client.Close()

    // 구독 객체 획득
    subscription := client.Subscription(subscriptionName)
    fmt.Printf("구독 [%s]에서 메시지 수신 시작...\n", subscriptionName)

    // 메시지를 수신하기 위해 Receive 메소드 사용
    err = subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
        decoded, err := base64.StdEncoding.DecodeString(msg.Data)
        if err != nil {
            log.Printf("메시지 디코딩 에러: %v", err)
            msg.Nack() // 디코딩 실패 시 Nack 호출
            return
        }
        fmt.Printf("수신 메시지: %s\n", string(decoded))
        msg.Ack() // 정상 처리 시 Ack 호출
    })
    if err != nil {
        log.Fatalf("subscription.Receive error: %v", err)
    }

    fmt.Println("메시지 수신 작업이 종료되었습니다.")
}

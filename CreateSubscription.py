import requests
import json
import logging
from config import *

#################################
# 로깅 설정
#################################
logging.basicConfig(
    filename=LOG_FILENAME,
    level=LOG_LEVEL,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

#################################
# 서브스크립션 생성 함수
#################################
def create_subscription(subscription_name, topic_name, ack_deadline=10, retention_duration="432000s", max_delivery_attempt=5):
    """
    서브스크립션 생성 함수
    :param subscription_name: 서브스크립션 이름
    :param topic_name: 연결할 토픽 이름
    :param ack_deadline: Ack 대기 시간 (초 단위)
    :param retention_duration: 메시지 보존 기간 (초 단위, 5일 = 432000초)
    :param max_delivery_attempt: 메시지 재전송 횟수
    """
    url = f"{PUBSUB_ENDPOINT_URL}/v1/domains/{PUBSUB_DOMAIN_ID}/projects/{PUBSUB_PROJECT_ID}/subscriptions/{subscription_name}"
    
    headers = {
        "Credential-ID": PUBSUB_CREDENTIAL_ID,
        "Credential-Secret": PUBSUB_CREDENTIAL_SECRET,
        "Content-Type": "application/json"
    }
    
    payload = {
        "subscription": {
            "topic": topic_name,
            "ackDeadlineSeconds": ack_deadline,
            "messageRetentionDuration": retention_duration,
            "maxDeliveryAttempt": max_delivery_attempt
        }
    }
    
    try:
        response = requests.put(url, headers=headers, data=json.dumps(payload))
        if response.status_code in [200, 201]:
            subscription_info = response.json()
            logging.info(f"Subscription '{subscription_name}' created successfully.")
            print(f"Subscription '{subscription_name}' created successfully.")
            print("Response:", json.dumps(subscription_info, indent=4))
        else:
            logging.error(f"Failed to create subscription. Status Code: {response.status_code}, Response: {response.text}")
            print(f"Failed to create subscription. Status Code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        logging.error(f"Exception while creating subscription: {e}")
        print(f"Exception while creating subscription: {e}")

#################################
# 메인 함수
#################################
def main():
    # 서브스크립션 이름과 연결할 토픽 이름 설정
    subscription_name = "Test-Subscription"  # 실제 서브스크립션 이름
    topic_name = "Test-Topic-lsh"           # 연결할 토픽 이름

    # 서브스크립션 생성 호출
    create_subscription(
        subscription_name=subscription_name,
        topic_name=topic_name,
        ack_deadline=30,                 # Ack 대기 시간
        retention_duration="432000s",    # 메시지 보존 기간: 5일
        max_delivery_attempt=3           # 메시지 재전송 횟수: 3회
    )

if __name__ == "__main__":
    main()

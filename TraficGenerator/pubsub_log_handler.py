# pubsub_log_handler.py

import logging
import requests
import base64

class PubSubLogHandler(logging.Handler):
    """
    Python logging Handler that sends log records to a Kakao Cloud Pub/Sub topic.
    """
    def __init__(self, domain_id, project_id, topic_name,
                 credential_id, credential_secret,
                 pubsub_endpoint="https://pub-sub.kr-central-2.kakaocloud.com",
                 level=logging.NOTSET):
        super().__init__(level)
        self.domain_id = domain_id
        self.project_id = project_id
        self.topic_name = topic_name
        self.credential_id = credential_id
        self.credential_secret = credential_secret
        self.pubsub_endpoint = pubsub_endpoint

        # 미리 만든 publish URL
        self.publish_url = (
            f"{self.pubsub_endpoint}/v1/domains/{self.domain_id}"
            f"/projects/{self.project_id}/topics/{self.topic_name}/publish"
        )

    def emit(self, record: logging.LogRecord):
        try:
            # 1) 로그 메시지 포맷팅
            log_msg = self.format(record)
            # ex) 2025-01-08 06:22:20,138 - INFO - [user_xxx] GET /cart/view => 200

            # 2) Base64 인코딩
            data_b64 = base64.b64encode(log_msg.encode("utf-8")).decode("utf-8")

            # 3) Request Body
            body = {
                "messages": [
                    {
                        "data": data_b64,
                        "attributes": {
                            "source": "traffic_generator_logger",
                            "loglevel": record.levelname
                        }
                    }
                ]
            }

            # 4) 인증 헤더
            headers = {
                "Credential-ID": self.credential_id,
                "Credential-Secret": self.credential_secret,
                "Content-Type": "application/json"
            }

            # 5) Pub/Sub Publish 호출
            resp = requests.post(self.publish_url, headers=headers, json=body)
            resp.raise_for_status()
            # 만약 응답 상태/내용까지 로그로 남기고 싶으면 아래처럼:
            # print(f"PubSub publish success: {resp.json()}")

        except Exception:
            # Handler 내부에서 예외 발생 시 logging 시스템 전체가 죽지 않도록 예외 무시 or 출력
            self.handleError(record)

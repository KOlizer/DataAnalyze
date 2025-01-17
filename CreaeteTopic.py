import requests
import json

dtfrrysdfysfdy
# 설정
ENDPOINT_URL = 'https://pub-sub.kr-central-2.kakaocloud.com' # 문서 참고
DOMAIN_ID = 'fa22d0db818f48829cf8b7849e3a0a26'      # 프로젝트가 속한 조직 ID, IAM참고
PROJECT_ID = '0aa67b93c3ec48e587a51c9f842ca407'    # 카카오클라우드 프로젝트 ID, IAM참고
TOPIC_NAME = 'Log-Topic'  # 생성할 토픽 이름

CREDENTIAL_ID = '23630c9edc9b4a1bad341eee37268557'        # 액세스 키 ID
CREDENTIAL_SECRET = 'eb2e612b5c85450ece8838d76ea6bb2831d25eb3b00f5d33ee1debab8ab25ce197d94d'  # 보안 액세스 키

# URI 구성
uri = f"{ENDPOINT_URL}/v1/domains/{DOMAIN_ID}/projects/{PROJECT_ID}/topics/{TOPIC_NAME}"

# 헤더 설정
headers = {
    'Credential-ID': CREDENTIAL_ID,
    'Credential-Secret': CREDENTIAL_SECRET,
    'Content-Type': 'application/json'
}

# 요청 본문 구성
payload = {
    "topic": {
        "description": "Log Topic",
        "messageRetentionDuration": "604800s"  # 7일
    }
}

# 토픽 생성 API 호출
response = requests.put(uri, headers=headers, data=json.dumps(payload))

if response.status_code in [200, 201]:
    print('Log Ingestion Topic created successfully.')
    try:
        # 응답 본문 출력
        response_json = response.json()
        print('Response:', response_json)
    except json.JSONDecodeError:
        print('Response body is empty or not in JSON format.')
        print('Response Text:', response.text)
else:
    print(f'Failed to create topic: {response.status_code}')
    print('Response:', response.text)
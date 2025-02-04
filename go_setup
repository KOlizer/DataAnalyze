# 홈 디렉토리에 config.yml이 있다고 가정

# 시스템 패키지 목록을 최신 상태로 업데이트
echo "시스템 패키지 목록을 업데이트하고, Go 1.20.5를 설치하며 Go 환경을 설정합니다."
sudo apt update
wget https://go.dev/dl/go1.20.5.linux-amd64.tar.gz
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf go1.20.5.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
source ~/.bashrc
go version
--------------------------------------------------------
# 새로운 디렉토리 gosdk 생성하고 이동
echo "'gosdk' 디렉토리를 생성하고 이동합니다."
mkdir gosdk && cd gosdk
# Pub/Sub SDK 다운로드
echo "Pub/Sub SDK를 다운로드하고 압축을 풉니다."
wget https://objectstorage.kr-central-2.kakaocloud.com/v1/e9130193fc734337b2b0c1da50e44395/pubsub-sdk/go/v1.0.0/pubsub.tgz
tar -xf pubsub.tgz
--------------------------------------------------------
# Go 모듈에 필요한 의존성 추가
echo "Go 모듈에 필요한 의존성을 추가하고, SDK 경로를 수정합니다."
go mod edit -require github.kakaoenterprise.in/cloud-platform/kc-pub-sub-sdk-go@v1.0.0
go mod edit -replace github.kakaoenterprise.in/cloud-platform/kc-pub-sub-sdk-go@v1.0.0=/home/ubuntu/gosdk
--------------------------------------------------------
# cmd 디렉토리 생성 후 이동
echo "'cmd' 디렉토리를 생성하고 이동합니다."
mkdir cmd
cd cmd
--------------------------------------------------------
# 필요한 Go 파일들을 다운로드
echo "필요한 Go 파일(config.go, publisher.go, subscriber.go)을 다운로드합니다."
wget -O config.go https://raw.githubusercontent.com/KOlizer/syu-DataAnalyze/main/TrafficGenerator/GO_SDK/config.go
wget -O publisher.go https://raw.githubusercontent.com/KOlizer/syu-DataAnalyze/main/TrafficGenerator/GO_SDK/publisher.go
wget -O subscriber.go https://raw.githubusercontent.com/KOlizer/syu-DataAnalyze/main/TrafficGenerator/GO_SDK/subscriber.go
--------------------------------------------------------
# Go 패키지 gopkg.in/yaml.v2 설치
echo "gopkg.in/yaml.v2 패키지를 설치합니다."
go get gopkg.in/yaml.v2
--------------------------------------------------------
# 의존성 정리
echo "Go 모듈의 의존성을 정리합니다."
go mod tidy

echo "Go SDK 환경 설정을 완료하였습니다."





# --------------------------------------------------------
# # publisher.go 빌드
# echo "publisher.go 파일을 빌드합니다."
# go build -o publisher config.go publisher.go
# # publisher 실행
# echo "publisher를 실행합니다."
# ./publisher
# --------------------------------------------------------
# # subscriber.go 빌드
# echo "subscriber.go 파일을 빌드합니다."
# go build -o subscriber config.go subscriber.go
# # subscriber 실행
# echo "subscriber를 실행합니다."
# ./subscriber
# --------------------------------------------------------

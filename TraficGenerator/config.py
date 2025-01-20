# config.py

import logging

#################################
# 로그 설정
#################################
LOG_FILENAME = "traffic_generator.log"
LOG_LEVEL = logging.INFO

#################################
# 스레드 및 사용자 수 관련 설정
#################################
NUM_USERS = 20         # 시뮬레이션할 사용자(스레드) 수
MAX_THREADS = 5        # 동시에 실행할 최대 스레드 수
ACTIONS_PER_USER = 30  # 각 사용자가 수행할 최대 상태 전이 횟수

#################################
# API 서버 정보
#################################
API_BASE_URL = "http://210.109.52.240/"  # 실제 API 서버 IP/도메인
TIME_SLEEP_RANGE = (0.1, 1.0)  # 상태 전이 사이의 sleep 시간 범위(초)

#################################
# 엔드포인트 경로
#################################
API_ENDPOINTS = {
    "ADD_USER":          "add_user",
    "DELETE_USER":       "delete_user",
    "LOGIN":             "login",
    "LOGOUT":            "logout",
    "PRODUCTS":          "products",
    "PRODUCT_DETAIL":    "product",     # /product?id=xxx
    "SEARCH":            "search",      # /search?query=xxx
    "CHECKOUT_HISTORY":  "checkout_history",
    "CATEGORIES":        "categories",
    "CATEGORY":          "category",    # /category?name=xxx
    "CART_VIEW":         "cart/view",
    "CART_ADD":          "cart/add",
    "CART_REMOVE":       "cart/remove",
    "CHECKOUT":          "checkout",
    "ADD_REVIEW":        "add_review",
    "ERROR_PAGE":        "error"
}

#################################
# 나이 구간 임계값
#################################
AGE_THRESHOLD_YOUNG = 25   # 25세 미만 -> young
AGE_THRESHOLD_MIDDLE = 50  # 25세 이상 ~ 50세 미만 -> middle
# 50세 이상 -> old

#################################
# 상위 상태 전이 표
#################################
STATE_TRANSITIONS = {
    # ─────────────────────────────────────
    # A) 비로그인 상태
    # ─────────────────────────────────────

    # 가입 안 된 상태
    "Anon_NotRegistered": {
        # 자기 자신(하위머신: 비로그인용)을 실행 후 다시 복귀
        "Anon_NotRegistered": 0.3,
        # 가입(성공) → Anon_Registered
        "Anon_Registered": 0.5,
        # 탈퇴 -> 굳이 의미 없으니 0.0 ~ 0.1 정도(예시)
        "Unregistered": 0.0,
        # 종료
        "Done": 0.2
    },

    # 가입은 했으나 로그인 안 됨
    "Anon_Registered": {
        # 자기 자신(하위머신: 비로그인용)을 실행 후 복귀
        "Anon_Registered": 0.3,
        # 로그인 성공 → Logged_In
        "Logged_In": 0.5,
        # 회원 탈퇴
        "Unregistered": 0.1,
        # 종료
        "Done": 0.1
    },

    # ─────────────────────────────────────
    # B) 로그인 상태
    # ─────────────────────────────────────
    "Logged_In": {
        "Logged_In": 0.7,
        "Logged_Out": 0.1,
        "Unregistered": 0.1,
        "Done": 0.1
    },

    # ─────────────────────────────────────
    # C) 로그아웃 상태
    # ─────────────────────────────────────
    "Logged_Out": {
        "Unregistered": 0.1,
        "Anon_Registered": 0.1,
        "Done": 0.8
    },

    # ─────────────────────────────────────
    # D) 탈퇴 완료 및 종료
    # ─────────────────────────────────────
    "Unregistered": {
        "Done": 1.0
    },

    "Done": {}
}


#################################
# 비로그인 하위머신: ANON_SUB_TRANSITIONS
#################################
ANON_SUB_TRANSITIONS = {
    # 하위머신 진입점
    "Anon_Sub_Initial": {
        "Anon_Sub_Main":       0.2,
        "Anon_Sub_Products":   0.2,
        "Anon_Sub_Categories": 0.2,
        "Anon_Sub_Search":     0.2,
        "Anon_Sub_Done":       0.2
    },

    # 메인 페이지 접근( / ) 
    "Anon_Sub_Main": {
        # 여기서 그냥 메인 페이지 머무르거나
        "Anon_Sub_Main": 0.1,
        # 특정 상품 목록으로
        "Anon_Sub_Products": 0.2,
        # 검색으로
        "Anon_Sub_Search": 0.2,
        # 카테고리 목록으로
        "Anon_Sub_Categories": 0.2,
        # 종종 에러페이지
        "Anon_Sub_Error": 0.1,
        # 하위머신 탈출
        "Anon_Sub_Done": 0.2
    },

    # 상품 목록 ( /products )
    "Anon_Sub_Products": {
        # 특정 상품 열람
        "Anon_Sub_ViewProduct": 0.2,
        # 다른 상품 목록 조회(=self)
        "Anon_Sub_Products": 0.2,
        # 카테고리 목록
        "Anon_Sub_Categories": 0.1,
        # 검색
        "Anon_Sub_Search": 0.2,
        # 에러페이지
        "Anon_Sub_Error": 0.1,
        # 하위머신 Done
        "Anon_Sub_Done": 0.2
    },

    # 상품 상세 보기( /product?id=xxx )
    "Anon_Sub_ViewProduct": {
        # 다시 다른 상품을 보러갈 수도
        "Anon_Sub_Products": 0.2,
        # 검색
        "Anon_Sub_Search": 0.2,
        # 카테고리
        "Anon_Sub_Categories": 0.1,
        # 에러
        "Anon_Sub_Error": 0.1,
        # Done
        "Anon_Sub_Done": 0.4
    },

    # 카테고리 목록( /categories )
    "Anon_Sub_Categories": {
        # 특정 카테고리 접근
        "Anon_Sub_CategoryList": 0.4,
        # 검색
        "Anon_Sub_Search": 0.2,
        # 에러
        "Anon_Sub_Error": 0.1,
        # 끝
        "Anon_Sub_Done": 0.3
    },

    # 특정 카테고리 상품 목록( /category?name=... )
    "Anon_Sub_CategoryList": {
        # 또 다른 카테고리
        "Anon_Sub_Categories": 0.2,
        # 특정 상품 상세
        "Anon_Sub_ViewProduct": 0.3,
        # 에러
        "Anon_Sub_Error": 0.1,
        # 끝
        "Anon_Sub_Done": 0.4
    },

    # 검색( /search?query=... )
    "Anon_Sub_Search": {
        # 검색 결과에서 특정 상품 보기
        "Anon_Sub_ViewProduct": 0.3,
        # 다시 검색(=self)
        "Anon_Sub_Search": 0.2,
        # 에러
        "Anon_Sub_Error": 0.1,
        # 끝
        "Anon_Sub_Done": 0.4
    },

    # 에러페이지( /error )
    "Anon_Sub_Error": {
        # 메인 페이지로 이동
        "Anon_Sub_Main": 0.3,
        # 상품 목록
        "Anon_Sub_Products": 0.2,
        # Done
        "Anon_Sub_Done": 0.5
    },

    # 하위머신 종료
    "Anon_Sub_Done": {}
}


#################################
# 로그인 상태에서 발생 가능한 하위머신
#################################
LOGGED_SUB_TRANSITIONS = {
    "Login_Sub_Initial": {
        "Login_Sub_ViewCart":        0.2,
        "Login_Sub_CheckoutHistory": 0.1,
        "Login_Sub_CartAdd":         0.2,
        "Login_Sub_CartRemove":      0.1,
        "Login_Sub_Checkout":        0.1,
        "Login_Sub_AddReview":       0.1,
        "Login_Sub_Error":           0.1,
        "Login_Sub_Done":            0.1
    },

    # 장바구니 보기( /cart/view )
    "Login_Sub_ViewCart": {
        # 계속 장바구니에 머물기
        "Login_Sub_ViewCart": 0.1,
        # 상품 추가
        "Login_Sub_CartAdd": 0.2,
        # 상품 제거
        "Login_Sub_CartRemove": 0.2,
        # 결제
        "Login_Sub_Checkout": 0.1,
        # 에러
        "Login_Sub_Error": 0.1,
        # done
        "Login_Sub_Done": 0.3
    },

    # 결제 이력( /checkout_history )
    "Login_Sub_CheckoutHistory": {
        # 다시 결제 이력
        "Login_Sub_CheckoutHistory": 0.1,
        # 장바구니 보기
        "Login_Sub_ViewCart": 0.2,
        # 상품 추가
        "Login_Sub_CartAdd": 0.1,
        # 에러
        "Login_Sub_Error": 0.1,
        # done
        "Login_Sub_Done": 0.5
    },

    # 장바구니에 상품 추가(POST /cart/add)
    "Login_Sub_CartAdd": {
        # 다시 장바구니 추가(=self)
        "Login_Sub_CartAdd": 0.1,
        # 장바구니 보기
        "Login_Sub_ViewCart": 0.3,
        # 결제
        "Login_Sub_Checkout": 0.1,
        # 에러
        "Login_Sub_Error": 0.1,
        # done
        "Login_Sub_Done": 0.4
    },

    # 장바구니 상품 제거(POST /cart/remove)
    "Login_Sub_CartRemove": {
        # 다시 카트 제거
        "Login_Sub_CartRemove": 0.1,
        # 장바구니 보기
        "Login_Sub_ViewCart": 0.3,
        # 결제
        "Login_Sub_Checkout": 0.1,
        # 에러
        "Login_Sub_Error": 0.1,
        # done
        "Login_Sub_Done": 0.4
    },

    # 결제(POST /checkout)
    "Login_Sub_Checkout": {
        # 결제 직후 다시 결제(=self)는 드물게 0.0 or 낮게
        "Login_Sub_Checkout": 0.0,
        # 결제이력
        "Login_Sub_CheckoutHistory": 0.2,
        # 장바구니
        "Login_Sub_ViewCart": 0.2,
        # 리뷰 작성
        "Login_Sub_AddReview": 0.1,
        # 에러
        "Login_Sub_Error": 0.1,
        # done
        "Login_Sub_Done": 0.4
    },

    # 리뷰 작성(POST /add_review)
    "Login_Sub_AddReview": {
        # 또 다른 리뷰
        "Login_Sub_AddReview": 0.1,
        # 장바구니로
        "Login_Sub_ViewCart": 0.2,
        # 결제 이력
        "Login_Sub_CheckoutHistory": 0.1,
        # 에러
        "Login_Sub_Error": 0.1,
        # done
        "Login_Sub_Done": 0.5
    },

    # 에러( /error )
    "Login_Sub_Error": {
        # 다시 장바구니
        "Login_Sub_ViewCart": 0.2,
        # 리뷰 작성
        "Login_Sub_AddReview": 0.1,
        # 결제
        "Login_Sub_Checkout": 0.1,
        # done
        "Login_Sub_Done": 0.6
    },

    # 하위머신 종료
    "Login_Sub_Done": {}
}


#################################
# 카테고리 선호도
#################################
CATEGORY_PREFERENCE = {
    "F": {
        "young":  ["Fashion", "Electronics", "Books"],
        "middle": ["Fashion", "Home", "Books"],
        "old":    ["Home", "Books"]
    },
    "M": {
        "young":  ["Electronics", "Gaming", "Fashion"],
        "middle": ["Electronics", "Home", "Gaming"],
        "old":    ["Home", "Books"]
    }
}

#################################
# 검색어 목록 (공용)
#################################
SEARCH_KEYWORDS = [
    "Bluetooth", "Laptop", "Fashion", "Camera", "Book", "Home",
    "Coffee", "Mouse", "Sneakers", "Bag", "Sunglasses", "Mug",
    "cofee", "blu tooth", "iphon", "labtop", "rayban" # 오타 섞기
]

#################################
# Pub/Sub 설정
#################################
PUBSUB_ENDPOINT_URL = 'https://pub-sub.kr-central-2.kakaocloud.com' # 문서 참고
PUBSUB_DOMAIN_ID = 'fa22d0db818f48829cf8b7849e3a0a26'      # 프로젝트가 속한 조직 ID, IAM참고
PUBSUB_PROJECT_ID = '0aa67b93c3ec48e587a51c9f842ca407'    # 카카오클라우드 프로젝트 ID, IAM참고
PUBSUB_TOPIC_NAME = 'Test-Topic-lsh'  # 생성한 토픽 이름

CREDENTIAL_ID = '23630c9edc9b4a1bad341eee37268557'        # 액세스 키 ID
CREDENTIAL_SECRET = 'eb2e612b5c85450ece8838d76ea6bb2831d25eb3b00f5d33ee1debab8ab25ce197d94d'  # 보안 액세스 키

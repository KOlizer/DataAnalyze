# config.py

import yaml
import logging
import os

# YAML 파일 로드
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# 민감한 정보는 환경 변수에서 로드 (보안 강화)
CREDENTIAL_ID = os.getenv("CREDENTIAL_ID", config['pubsub']['credential_id'])
CREDENTIAL_SECRET = os.getenv("CREDENTIAL_SECRET", config['pubsub']['credential_secret'])

# 공용 설정 로드
PUBSUB_ENDPOINT = config['pubsub']['endpoint']
DOMAIN_ID = config['pubsub']['domain_id']
PROJECT_ID = config['pubsub']['project_id']
TOPIC_NAME = config['pubsub']['topic_name']
TOPIC_NAME_MK = config['pubsub']['topic_name_mk']
TOPIC_DESCRIPTION = config['pubsub']['topic_description']
TOPIC_RETENTION_DURATION = config['pubsub']['topic_retention_duration']

SUBSCRIPTION_NAME = config['subscription']['name']

OBJECT_STORAGE_SUBSCRIPTION_NAME = config['object_storage_subscription']['name']
OBJECT_STORAGE_BUCKET = config['object_storage_subscription']['bucket']
OBJECT_STORAGE_EXPORT_INTERVAL_MIN = config['object_storage_subscription']['export_interval_min']
OBJECT_STORAGE_FILE_PREFIX = config['object_storage_subscription']['file_prefix']
OBJECT_STORAGE_FILE_SUFFIX = config['object_storage_subscription']['file_suffix']
OBJECT_STORAGE_CHANNEL_COUNT = config['object_storage_subscription']['channel_count']
OBJECT_STORAGE_MAX_CHANNEL_COUNT = config['object_storage_subscription']['max_channel_count']
OBJECT_STORAGE_IS_EXPORT_ENABLED = config['object_storage_subscription']['is_export_enabled']

LOG_FILENAME = config['logging']['filename']
LOG_LEVEL = getattr(logging, config['logging']['level'].upper(), logging.INFO)

NUM_USERS = config['threads']['num_users']
MAX_THREADS = config['threads']['max_threads']
ACTIONS_PER_USER = config['threads']['actions_per_user']

API_BASE_URL = config['api']['base_url']
TIME_SLEEP_RANGE = (config['api']['time_sleep_range']['min'], config['api']['time_sleep_range']['max'])

AGE_THRESHOLD_YOUNG = config['age_threshold']['young']
AGE_THRESHOLD_MIDDLE = config['age_threshold']['middle']

# 구독 엔드포인트 URL 구성
SUBSCRIPTION_ENDPOINT = f"{PUBSUB_ENDPOINT}/v1/domains/{DOMAIN_ID}/projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_NAME}/pull"

# 토픽 엔드포인트 URL 구성
TOPIC_ENDPOINT = f"{PUBSUB_ENDPOINT}/v1/domains/{DOMAIN_ID}/projects/{PROJECT_ID}/topics/{TOPIC_NAME}"


# 고유한 설정 (변하지 않는 상수)
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

# 비로그인 하위머신: ANON_SUB_TRANSITIONS
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

# 로그인 상태에서 발생 가능한 하위머신: LOGGED_SUB_TRANSITIONS
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

# 카테고리 선호도
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

# 검색어 목록 (공용)
SEARCH_KEYWORDS = [
    "Bluetooth", "Laptop", "Fashion", "Camera", "Book", "Home",
    "Coffee", "Mouse", "Sneakers", "Bag", "Sunglasses", "Mug",
    "cofee", "blu tooth", "iphon", "labtop", "rayban" # 오타 섞기
]

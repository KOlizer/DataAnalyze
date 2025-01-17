# traffic_generator.py

import requests
import threading
import time
import random
import uuid
import logging
import base64
import json  # Ensure json is imported

# config.py 불러오기
from config import *

#################################
# Pub/Sub 메시지 게시 함수
#################################
def publish_messages(messages):
    """
    Pub/Sub 토픽에 메시지를 게시하는 함수
    :param messages: 게시할 메시지 리스트 (dict 형식)
    """
    url = f"{PUBSUB_ENDPOINT_URL}/v1/domains/{PUBSUB_DOMAIN_ID}/projects/{PUBSUB_PROJECT_ID}/topics/{PUBSUB_TOPIC_NAME}/publish"
    
    headers = {
        "Credential-ID": PUBSUB_CREDENTIAL_ID,
        "Credential-Secret": PUBSUB_CREDENTIAL_SECRET,
        "Content-Type": "application/json"
    }
    
    payload = {
        "messages": []
    }
    
    for msg in messages:
        # 메시지 데이터는 Base64로 인코딩되어야 함
        encoded_data = base64.b64encode(msg["data"].encode('utf-8')).decode('utf-8')
        message = {
            "data": encoded_data
        }
        if "attributes" in msg:
            message["attributes"] = msg["attributes"]
        payload["messages"].append(message)
    
    try:
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        if response.status_code in [200, 201]:
            response_json = response.json()
            logging.info(f"Published messages successfully. Message IDs: {response_json.get('messageIds')}")
        else:
            logging.error(f"Failed to publish messages: {response.status_code}, Response: {response.text}")
    except Exception as e:
        logging.error(f"Exception while publishing messages: {e}")

def publish_event_message(user_id, event_type, details):
    """
    특정 이벤트를 메시지로 Pub/Sub 토픽에 게시
    :param user_id: 이벤트를 발생시킨 사용자 ID
    :param event_type: 이벤트 유형 (예: 'login', 'logout', 'purchase')
    :param details: 이벤트에 대한 상세 정보 (dict)
    """
    message_data = json.dumps({
        "user_id": user_id,
        "event_type": event_type,
        "details": details
    })
    
    message = {
        "data": message_data,
        "attributes": {
            "user_id": user_id,
            "event_type": event_type
        }
    }
    
    publish_messages([message])

#################################
# 전역 상품/카테고리 캐시
#################################
products_cache = []
categories_cache = []

#################################
# 로깅 설정
#################################
logging.basicConfig(
    filename=LOG_FILENAME,
    level=LOG_LEVEL,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

#################################
# 나이 구간 판단 함수
#################################
def get_age_segment(age: int) -> str:
    if age < AGE_THRESHOLD_YOUNG:
        return "young"
    elif age < AGE_THRESHOLD_MIDDLE:
        return "middle"
    else:
        return "old"

#################################
# 상품/카테고리 데이터 가져오기
#################################
def fetch_products(api_base_url: str):
    global products_cache
    headers = {"Accept": "application/json"}
    try:
        url = api_base_url + API_ENDPOINTS["PRODUCTS"]
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                products_cache = data
            elif isinstance(data, dict):
                products_cache = data.get("products", [])
            else:
                products_cache = []
            logging.info(f"Fetched {len(products_cache)} products.")
        else:
            logging.error(f"Failed to fetch products: {resp.status_code}, content={resp.text}")
    except Exception as e:
        logging.error(f"Exception while fetching products: {e}")

def fetch_categories(api_base_url: str):
    global categories_cache
    headers = {"Accept": "application/json"}
    try:
        url = api_base_url + API_ENDPOINTS["CATEGORIES"]
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                categories_cache = data
            elif isinstance(data, dict):
                categories_cache = data.get("categories", [])
            else:
                categories_cache = []
            logging.info(f"Fetched {len(categories_cache)} categories.")
        else:
            logging.error(f"Failed to fetch categories: {resp.status_code}, content={resp.text}")
    except Exception as e:
        logging.error(f"Exception while fetching categories: {e}")

#################################
# 확률 전이 공통 함수
#################################
def pick_next_state(prob_dict: dict) -> str:
    states = list(prob_dict.keys())
    probs = list(prob_dict.values())
    return random.choices(states, weights=probs, k=1)[0]

#################################
# 선호 카테고리 상품 선택
#################################
def pick_preferred_product_id(gender: str, age_segment: str) -> str:
    if not products_cache:
        return "101"  # fallback
    cat_list = CATEGORY_PREFERENCE.get(gender, {}).get(age_segment, [])
    filtered = [p for p in products_cache if p.get("category", "") in cat_list]
    if filtered:
        chosen = random.choice(filtered)
        return chosen.get("id", "101")
    else:
        chosen = random.choice(products_cache)
        return chosen.get("id", "101")

#################################
# 실제 회원가입/로그인/로그아웃/탈퇴 시도
#################################
def try_register(session: requests.Session, user_id: str, gender: str, age_segment: str) -> bool:
    headers = {"Accept": "application/json"}
    payload = {
        "user_id": user_id,
        "name": f"TestUser_{user_id}",
        "email": f"{user_id}@example.com",
        "gender": gender,
        "age": str(random.randint(18, 70))
    }
    try:
        url = API_BASE_URL + API_ENDPOINTS["ADD_USER"]  # 예: /add_user
        r = session.post(url, data=payload, headers=headers)
        logging.info(f"[{user_id}] POST /add_user => {r.status_code}")
        if r.status_code == 201:
            # 성공 시 이벤트 게시
            publish_event_message(user_id, "register", {"status": "success"})
            return True
        else:
            # 실패 시 이벤트 게시
            publish_event_message(user_id, "register", {"status": "failed", "status_code": r.status_code})
            return False
    except Exception as e:
        logging.error(f"[{user_id}] register exception: {e}")
        publish_event_message(user_id, "register", {"status": "exception", "error": str(e)})
        return False

def try_login(session: requests.Session, user_id: str) -> bool:
    headers = {"Accept": "application/json"}
    payload = {"user_id": user_id}
    try:
        url = API_BASE_URL + API_ENDPOINTS["LOGIN"]  # 예: /login
        r = session.post(url, data=payload, headers=headers)
        logging.info(f"[{user_id}] POST /login => {r.status_code}")
        if 200 <= r.status_code < 300:
            # 성공 시 이벤트 게시
            publish_event_message(user_id, "login", {"status": "success"})
            return True
        else:
            # 실패 시 이벤트 게시
            publish_event_message(user_id, "login", {"status": "failed", "status_code": r.status_code})
            return False
    except Exception as e:
        logging.error(f"[{user_id}] login exception: {e}")
        publish_event_message(user_id, "login", {"status": "exception", "error": str(e)})
        return False

def try_logout(session: requests.Session, user_id: str) -> bool:
    headers = {"Accept": "application/json"}
    try:
        url = API_BASE_URL + API_ENDPOINTS["LOGOUT"]  # /logout
        r = session.post(url, headers=headers)
        logging.info(f"[{user_id}] POST /logout => {r.status_code}")
        if 200 <= r.status_code < 300:
            # 성공 시 이벤트 게시
            publish_event_message(user_id, "logout", {"status": "success"})
            return True
        else:
            # 실패 시 이벤트 게시
            publish_event_message(user_id, "logout", {"status": "failed", "status_code": r.status_code})
            return False
    except Exception as e:
        logging.error(f"[{user_id}] logout exception: {e}")
        publish_event_message(user_id, "logout", {"status": "exception", "error": str(e)})
        return False

def try_delete_user(session: requests.Session, user_id: str) -> bool:
    headers = {"Accept": "application/json"}
    payload = {"user_id": user_id}
    try:
        url = API_BASE_URL + API_ENDPOINTS["DELETE_USER"]  # /delete_user
        r = session.post(url, data=payload, headers=headers)
        logging.info(f"[{user_id}] POST /delete_user => {r.status_code}")
        if 200 <= r.status_code < 300:
            # 성공 시 이벤트 게시
            publish_event_message(user_id, "delete_user", {"status": "success"})
            return True
        else:
            # 실패 시 이벤트 게시
            publish_event_message(user_id, "delete_user", {"status": "failed", "status_code": r.status_code})
            return False
    except Exception as e:
        logging.error(f"[{user_id}] delete_user exception: {e}")
        publish_event_message(user_id, "delete_user", {"status": "exception", "error": str(e)})
        return False

#################################
# 비로그인 하위 FSM
#################################
def do_anon_sub_fsm(session: requests.Session, user_unique_id: str):
    sub_state = "Anon_Sub_Initial"
    while sub_state != "Anon_Sub_Done":
        logging.info(f"[{user_unique_id}] Anon Sub-FSM state = {sub_state}")
        perform_anon_sub_action(session, user_unique_id, sub_state)

        if sub_state not in ANON_SUB_TRANSITIONS:
            logging.warning(f"[{user_unique_id}] {sub_state} not in ANON_SUB_TRANSITIONS => break")
            break

        transitions = ANON_SUB_TRANSITIONS[sub_state]
        if not transitions:
            logging.warning(f"[{user_unique_id}] No next transitions => break")
            break

        next_sub = pick_next_state(transitions)
        logging.info(f"[{user_unique_id}] (AnonSub) {sub_state} -> {next_sub}")
        sub_state = next_sub

        # 예: 각 상태 전환 후 이벤트 게시
        publish_event_message(user_unique_id, "anon_sub_state_transition", {"current_state": sub_state})

        time.sleep(random.uniform(*TIME_SLEEP_RANGE))

def perform_anon_sub_action(session: requests.Session, user_unique_id: str, sub_state: str):
    headers = {"Accept": "application/json"}

    if sub_state == "Anon_Sub_Main":
        try:
            r = session.get(API_BASE_URL, headers=headers)
            logging.info(f"[{user_unique_id}] GET / => {r.status_code}")
            # 이벤트 게시
            publish_event_message(user_unique_id, "anon_sub_action", {"action": "access_main_page", "status_code": r.status_code})
        except Exception as e:
            logging.error(f"[{user_unique_id}] Anon_Sub_Main error: {e}")
            publish_event_message(user_unique_id, "anon_sub_action", {"action": "access_main_page", "status": "exception", "error": str(e)})

    elif sub_state == "Anon_Sub_Products":
        try:
            url = API_BASE_URL + API_ENDPOINTS["PRODUCTS"]
            resp = session.get(url, headers=headers)
            logging.info(f"[{user_unique_id}] GET /products => {resp.status_code}")
            # 이벤트 게시
            publish_event_message(user_unique_id, "anon_sub_action", {"action": "view_products", "status_code": resp.status_code})
        except Exception as e:
            logging.error(f"[{user_unique_id}] Anon_Sub_Products error: {e}")
            publish_event_message(user_unique_id, "anon_sub_action", {"action": "view_products", "status": "exception", "error": str(e)})

    elif sub_state == "Anon_Sub_ViewProduct":
        if products_cache:
            prod = random.choice(products_cache)
            pid = prod.get("id", "101")
            url = f"{API_BASE_URL}{API_ENDPOINTS['PRODUCT_DETAIL']}?id={pid}"
            try:
                r = session.get(url, headers=headers)
                logging.info(f"[{user_unique_id}] GET /product?id={pid} => {r.status_code}")
                # 이벤트 게시
                publish_event_message(user_unique_id, "anon_sub_action", {"action": "view_product_detail", "product_id": pid, "status_code": r.status_code})
            except Exception as err:
                logging.error(f"[{user_unique_id}] view product error: {err}")
                publish_event_message(user_unique_id, "anon_sub_action", {"action": "view_product_detail", "product_id": pid, "status": "exception", "error": str(err)})

    elif sub_state == "Anon_Sub_Categories":
        try:
            url = API_BASE_URL + API_ENDPOINTS["CATEGORIES"]
            r = session.get(url, headers=headers)
            logging.info(f"[{user_unique_id}] GET /categories => {r.status_code}")
            # 이벤트 게시
            publish_event_message(user_unique_id, "anon_sub_action", {"action": "view_categories", "status_code": r.status_code})
        except Exception as err:
            logging.error(f"[{user_unique_id}] categories error: {err}")
            publish_event_message(user_unique_id, "anon_sub_action", {"action": "view_categories", "status": "exception", "error": str(err)})

    elif sub_state == "Anon_Sub_CategoryList":
        if categories_cache:
            chosen_cat = random.choice(categories_cache)
            cat_url = f"{API_BASE_URL}{API_ENDPOINTS['CATEGORY']}?name={chosen_cat}"
            try:
                r = session.get(cat_url, headers=headers)
                logging.info(f"[{user_unique_id}] GET /category?name={chosen_cat} => {r.status_code}")
                # 이벤트 게시
                publish_event_message(user_unique_id, "anon_sub_action", {"action": "view_category", "category_name": chosen_cat, "status_code": r.status_code})
            except Exception as err:
                logging.error(f"[{user_unique_id}] category list error: {err}")
                publish_event_message(user_unique_id, "anon_sub_action", {"action": "view_category", "category_name": chosen_cat, "status": "exception", "error": str(err)})

    elif sub_state == "Anon_Sub_Search":
        q = random.choice(SEARCH_KEYWORDS)
        search_url = f"{API_BASE_URL}{API_ENDPOINTS['SEARCH']}?query={q}"
        try:
            r = session.get(search_url, headers=headers)
            logging.info(f"[{user_unique_id}] GET /search?query={q} => {r.status_code}")
            # 이벤트 게시
            publish_event_message(user_unique_id, "anon_sub_action", {"action": "search", "query": q, "status_code": r.status_code})
        except Exception as err:
            logging.error(f"[{user_unique_id}] search error: {err}")
            publish_event_message(user_unique_id, "anon_sub_action", {"action": "search", "query": q, "status": "exception", "error": str(err)})

    elif sub_state == "Anon_Sub_Error":
        try:
            err_url = API_BASE_URL + API_ENDPOINTS["ERROR_PAGE"]
            r = session.get(err_url, headers=headers)
            logging.info(f"[{user_unique_id}] GET /error => {r.status_code}")
            # 이벤트 게시
            publish_event_message(user_unique_id, "anon_sub_action", {"action": "trigger_error", "status_code": r.status_code})
        except Exception as err:
            logging.error(f"[{user_unique_id}] error page fail: {err}")
            publish_event_message(user_unique_id, "anon_sub_action", {"action": "trigger_error", "status": "exception", "error": str(err)})

    # Anon_Sub_Initial, Anon_Sub_Done => no specific action

#################################
# 로그인 하위 FSM
#################################
def do_logged_sub_fsm(session: requests.Session,
                      user_unique_id: str,
                      gender: str,
                      age_segment: str):
    sub_state = "Login_Sub_Initial"
    while sub_state != "Login_Sub_Done":
        logging.info(f"[{user_unique_id}] Logged Sub-FSM state = {sub_state}")
        perform_logged_sub_action(session, user_unique_id, sub_state, gender, age_segment)

        if sub_state not in LOGGED_SUB_TRANSITIONS:
            logging.warning(f"[{user_unique_id}] {sub_state} not in LOGGED_SUB_TRANSITIONS => break")
            break

        transitions = LOGGED_SUB_TRANSITIONS[sub_state]
        if not transitions:
            logging.warning(f"[{user_unique_id}] No next transitions => break")
            break

        next_sub = pick_next_state(transitions)
        logging.info(f"[{user_unique_id}] (LoggedSub) {sub_state} -> {next_sub}")
        sub_state = next_sub

        # 예: 각 상태 전환 후 이벤트 게시
        publish_event_message(user_unique_id, "logged_sub_state_transition", {"current_state": sub_state})

        time.sleep(random.uniform(*TIME_SLEEP_RANGE))

def perform_logged_sub_action(session: requests.Session,
                              user_unique_id: str,
                              sub_state: str,
                              gender: str,
                              age_segment: str):
    headers = {"Accept": "application/json"}

    if sub_state == "Login_Sub_ViewCart":
        url = API_BASE_URL + API_ENDPOINTS["CART_VIEW"]
        try:
            r = session.get(url, headers=headers)
            logging.info(f"[{user_unique_id}] GET /cart/view => {r.status_code}")
            # 이벤트 게시
            publish_event_message(user_unique_id, "logged_sub_action", {"action": "view_cart", "status_code": r.status_code})
        except Exception as e:
            logging.error(f"[{user_unique_id}] view cart error: {e}")
            publish_event_message(user_unique_id, "logged_sub_action", {"action": "view_cart", "status": "exception", "error": str(e)})

    elif sub_state == "Login_Sub_CheckoutHistory":
        url = API_BASE_URL + API_ENDPOINTS["CHECKOUT_HISTORY"]
        try:
            r = session.get(url, headers=headers)
            logging.info(f"[{user_unique_id}] GET /checkout_history => {r.status_code}")
            # 이벤트 게시
            publish_event_message(user_unique_id, "logged_sub_action", {"action": "view_checkout_history", "status_code": r.status_code})
        except Exception as e:
            logging.error(f"[{user_unique_id}] checkout_history error: {e}")
            publish_event_message(user_unique_id, "logged_sub_action", {"action": "view_checkout_history", "status": "exception", "error": str(e)})

    elif sub_state == "Login_Sub_CartAdd":
        pid = pick_preferred_product_id(gender, age_segment)
        qty = random.randint(1,3)
        payload = {"id": pid, "quantity": str(qty)}
        try:
            add_url = API_BASE_URL + API_ENDPOINTS["CART_ADD"]
            r = session.post(add_url, data=payload, headers=headers)
            logging.info(f"[{user_unique_id}] POST /cart/add (pid={pid}, qty={qty}) => {r.status_code}")
            if 200 <= r.status_code < 300:
                # 성공 시 이벤트 게시
                publish_event_message(user_unique_id, "logged_sub_action", {"action": "add_to_cart", "product_id": pid, "quantity": qty, "status": "success"})
            else:
                # 실패 시 이벤트 게시
                publish_event_message(user_unique_id, "logged_sub_action", {"action": "add_to_cart", "product_id": pid, "quantity": qty, "status": "failed", "status_code": r.status_code})
        except Exception as e:
            logging.error(f"[{user_unique_id}] cart add error: {e}")
            publish_event_message(user_unique_id, "logged_sub_action", {"action": "add_to_cart", "product_id": pid, "quantity": qty, "status": "exception", "error": str(e)})

    elif sub_state == "Login_Sub_CartRemove":
        # 우선 장바구니 조회
        view_url = API_BASE_URL + API_ENDPOINTS["CART_VIEW"]
        try:
            vr = session.get(view_url, headers=headers)
            if vr.status_code == 200:
                cart_data = vr.json()
                items = cart_data.get("cart_items", [])
                if items:
                    chosen_item = random.choice(items)
                    rid = chosen_item["product_id"]
                    rqty = random.randint(1, chosen_item["quantity"])
                    remove_payload = {"product_id": rid, "quantity": rqty}

                    # 기존 장바구니 수량을 고려하여 삭제
                    remove_url = API_BASE_URL + API_ENDPOINTS["CART_REMOVE"]
                    rr = session.post(remove_url, data=remove_payload, headers=headers)
                    logging.info(f"[{user_unique_id}] POST /cart/remove (pid={rid}, qty={rqty}) => {rr.status_code}")
                    if 200 <= rr.status_code < 300:
                        # 성공 시 이벤트 게시
                        publish_event_message(user_unique_id, "logged_sub_action", {"action": "remove_from_cart", "product_id": rid, "quantity": rqty, "status": "success"})
                    else:
                        # 실패 시 이벤트 게시
                        publish_event_message(user_unique_id, "logged_sub_action", {"action": "remove_from_cart", "product_id": rid, "quantity": rqty, "status": "failed", "status_code": rr.status_code})
                else:
                    logging.info(f"[{user_unique_id}] Cart empty => skip remove")
            else:
                logging.error(f"[{user_unique_id}] GET /cart/view fail => {vr.status_code}")
                publish_event_message(user_unique_id, "logged_sub_action", {"action": "view_cart_for_remove", "status": "failed", "status_code": vr.status_code})
        except Exception as e:
            logging.error(f"[{user_unique_id}] remove cart error: {e}")
            publish_event_message(user_unique_id, "logged_sub_action", {"action": "remove_from_cart", "status": "exception", "error": str(e)})

    elif sub_state == "Login_Sub_Checkout":
        check_url = API_BASE_URL + API_ENDPOINTS["CHECKOUT"]
        try:
            r = session.post(check_url, headers=headers)
            logging.info(f"[{user_unique_id}] POST /checkout => {r.status_code}")
            if 200 <= r.status_code < 300:
                # 성공 시 이벤트 게시
                publish_event_message(user_unique_id, "logged_sub_action", {"action": "checkout", "status": "success"})
            else:
                # 실패 시 이벤트 게시
                publish_event_message(user_unique_id, "logged_sub_action", {"action": "checkout", "status": "failed", "status_code": r.status_code})
        except Exception as e:
            logging.error(f"[{user_unique_id}] checkout error: {e}")
            publish_event_message(user_unique_id, "logged_sub_action", {"action": "checkout", "status": "exception", "error": str(e)})

    elif sub_state == "Login_Sub_AddReview":
        pid = pick_preferred_product_id(gender, age_segment)
        rating = random.randint(1,5)
        payload = {"product_id": pid, "rating": str(rating)}
        try:
            rev_url = API_BASE_URL + API_ENDPOINTS["ADD_REVIEW"]
            r = session.post(rev_url, data=payload, headers=headers)
            logging.info(f"[{user_unique_id}] POST /add_review (pid={pid},rating={rating}) => {r.status_code}")
            if 200 <= r.status_code < 300:
                # 성공 시 이벤트 게시
                publish_event_message(user_unique_id, "logged_sub_action", {"action": "add_review", "product_id": pid, "rating": rating, "status": "success"})
            else:
                # 실패 시 이벤트 게시
                publish_event_message(user_unique_id, "logged_sub_action", {"action": "add_review", "product_id": pid, "rating": rating, "status": "failed", "status_code": r.status_code})
        except Exception as e:
            logging.error(f"[{user_unique_id}] add review error: {e}")
            publish_event_message(user_unique_id, "logged_sub_action", {"action": "add_review", "product_id": pid, "rating": rating, "status": "exception", "error": str(e)})

    elif sub_state == "Login_Sub_Error":
        err_url = API_BASE_URL + API_ENDPOINTS["ERROR_PAGE"]
        try:
            rr = session.get(err_url, headers=headers)
            logging.info(f"[{user_unique_id}] GET /error => {rr.status_code}")
            # 이벤트 게시
            publish_event_message(user_unique_id, "logged_sub_action", {"action": "trigger_error", "status_code": rr.status_code})
        except Exception as e:
            logging.error(f"[{user_unique_id}] error page => {e}")
            publish_event_message(user_unique_id, "logged_sub_action", {"action": "trigger_error", "status": "exception", "error": str(e)})

#################################
# do_top_level_action_and_confirm
# (상위 상태 전이 시 실제 API 호출로 성공/실패 반영)
#################################
def do_top_level_action_and_confirm(
    session: requests.Session,
    current_state: str,
    proposed_next: str,
    user_id: str,
    gender: str,
    age_segment: str
) -> str:
    """
    실제 API 호출로 회원가입/로그인/로그아웃/탈퇴 시도.
    성공 => proposed_next 반환
    실패 => current_state로 롤백
    """
    # Anon_NotRegistered -> Anon_Registered => 회원가입
    if current_state == "Anon_NotRegistered" and proposed_next == "Anon_Registered":
        ok = try_register(session, user_id, gender, age_segment)
        return "Anon_Registered" if ok else "Anon_NotRegistered"

    # Anon_Registered -> Logged_In => 로그인
    if current_state == "Anon_Registered" and proposed_next == "Logged_In":
        ok = try_login(session, user_id)
        return "Logged_In" if ok else "Anon_Registered"

    # Logged_In -> Logged_Out => 로그아웃
    if current_state == "Logged_In" and proposed_next == "Logged_Out":
        ok = try_logout(session, user_id)
        return "Logged_Out" if ok else "Logged_In"

    # Logged_In -> Unregistered => 탈퇴
    if current_state == "Logged_In" and proposed_next == "Unregistered":
        ok = try_delete_user(session, user_id)
        return "Unregistered" if ok else "Logged_In"

    # Logged_Out -> Anon_Registered => 그냥 상태 전이(실제 API 없음)
    if current_state == "Logged_Out" and proposed_next == "Anon_Registered":
        return "Anon_Registered"

    # Logged_Out -> Unregistered => 탈퇴
    if current_state == "Logged_Out" and proposed_next == "Unregistered":
        ok = try_delete_user(session, user_id)
        return "Unregistered" if ok else "Logged_Out"

    # 그 외에는 그냥 전이
    return proposed_next

#################################
# 사용자 전체 로직
#################################
def run_user_simulation(user_idx: int):
    session = requests.Session()

    gender = random.choice(["F", "M"])
    age = random.randint(18,70)
    age_segment = get_age_segment(age)

    user_unique_id = f"user_{uuid.uuid4().hex[:6]}"
    logging.info(f"[{user_unique_id}] Start simulation. gender={gender}, age={age}")

    current_state = "Anon_NotRegistered"
    transition_count = 0

    while True:
        if transition_count >= ACTIONS_PER_USER:
            logging.info(f"[{user_unique_id}] Reached max transitions => end.")
            publish_event_message(user_unique_id, "simulation", {"status": "max_transitions_reached"})
            break

        if current_state == "Done":
            logging.info(f"[{user_unique_id}] state=Done => end.")
            publish_event_message(user_unique_id, "simulation", {"status": "done"})
            break

        # 상위 전이 후보
        if current_state not in STATE_TRANSITIONS:
            logging.error(f"[{user_unique_id}] no transitions from {current_state} => end.")
            publish_event_message(user_unique_id, "simulation", {"status": "invalid_state", "current_state": current_state})
            break

        possible_next = STATE_TRANSITIONS[current_state]
        if not possible_next:
            logging.warning(f"[{user_unique_id}] next_candidates empty => end.")
            publish_event_message(user_unique_id, "simulation", {"status": "no_next_candidates", "current_state": current_state})
            break

        proposed_next = pick_next_state(possible_next)
        logging.info(f"[{user_unique_id}] (Top) {current_state} -> proposed={proposed_next}")

        # 실제 API 호출로 성공/실패 반영
        actual_next = do_top_level_action_and_confirm(
            session=session,
            current_state=current_state,
            proposed_next=proposed_next,
            user_id=user_unique_id,
            gender=gender,
            age_segment=age_segment
        )
        if actual_next != current_state:
            logging.info(f"[{user_unique_id}] => confirmed next: {actual_next}")
            publish_event_message(user_unique_id, "top_level_state_transition", {"from": current_state, "to": actual_next})
            current_state = actual_next
        else:
            publish_event_message(user_unique_id, "top_level_state_transition_failed", {"current_state": current_state, "proposed_next": proposed_next})

        # 하위 FSM
        if current_state == "Anon_NotRegistered":
            do_anon_sub_fsm(session, user_unique_id)
        elif current_state == "Anon_Registered":
            do_anon_sub_fsm(session, user_unique_id)
        elif current_state == "Logged_In":
            do_logged_sub_fsm(session, user_unique_id, gender, age_segment)
        elif current_state == "Logged_Out":
            logging.info(f"[{user_unique_id}] (Top) state=Logged_Out => no sub-FSM")
            publish_event_message(user_unique_id, "sub_fsm", {"state": "Logged_Out"})
        elif current_state == "Unregistered":
            logging.info(f"[{user_unique_id}] user unregistered => next=Done")
            publish_event_message(user_unique_id, "unregister", {"status": "done"})
            current_state = "Done"

        transition_count += 1
        time.sleep(random.uniform(*TIME_SLEEP_RANGE))

    logging.info(f"[{user_unique_id}] Simulation ended. final={current_state}")

#################################
# 메시지 게시 예제 함수
#################################
def publish_test_messages():
    """
    테스트용 메시지를 Pub/Sub 토픽에 게시하는 예제 함수
    """
    test_messages = [
        {
            "data": "Test message 1",
            "attributes": {
                "key1": "value1",
                "key2": "value2"
            }
        },
        {
            "data": "Test message 2"
        }
    ]
    publish_messages(test_messages)

#################################
# 멀티 스레드 실행
#################################
semaphore = threading.Semaphore(MAX_THREADS)

def user_thread(idx: int):
    with semaphore:
        run_user_simulation(idx)
        # 각 사용자 시뮬레이션 후 메시지 게시 (예: 로그 적재)
        log_message = f"User {idx} simulation completed."
        messages = [
            {
                "data": log_message,
                "attributes": {
                    "user_id": f"user_{idx}",
                    "event": "simulation_complete"
                }
            }
        ]
        publish_messages(messages)

def main():
    # 초기 데이터
    fetch_products(API_BASE_URL)
    fetch_categories(API_BASE_URL)

    threads = []
    for i in range(NUM_USERS):
        t = threading.Thread(target=user_thread, args=(i,))
        threads.append(t)
        t.start()
        time.sleep(0.05)

    for t in threads:
        t.join()

    logging.info("All user threads finished.")

if __name__ == "__main__":
    main()
    print("Traffic generation completed. Check the log file for details.")

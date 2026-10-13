import asyncio
from typing import Any, Dict, List
from sqlalchemy import select, update, func
from sqlalchemy.orm.attributes import flag_modified
from taskiq_redis import RedisAsyncResultBackend, ListQueueBroker
from src.services.telegram.tg import tg_service
from src.core.config import settings
from src.core.logging import setup_logging
from src.core.redis_client import redis_manager
from src.db.database import async_session_maker
from src.db.models import Dialogue, AppSettings, Account
from src.services.pact.pact_api import pact_api
from src.core.redis_client import scheduler
from decimal import Decimal
from src.core.logging import setup_logging, logger
from src.logic.graph import app_graph
from src.logic.states import Steps, DialogueState
from src.logic.graph import SETTINGS_DATA
from src.services.amocrm.amo_api import amo_api
from src.services.telegram.tg import tg_service
import time
import copy
from datetime import datetime, timezone, timedelta

def get_msk_time() -> str:
    """Возвращает текущее время по Москве в нужном формате"""
    msk_tz = timezone(timedelta(hours=3))
    return datetime.now(msk_tz).strftime("%Y-%m-%d %H:%M:%S MSK")

def get_progress_bar(count: int, total: int = 3) -> str:
    """Генерирует визуальный прогресс-бар из эмодзи"""
    filled = "🟢" * min(count, total)
    empty = "⚪️" * max(0, total - count)
    return f"{filled}{empty}"

result_backend = RedisAsyncResultBackend(redis_url=settings.REDIS_URL)
broker = (
    ListQueueBroker(url=settings.REDIS_URL)
    .with_result_backend(result_backend)
)

logger = setup_logging("worker")

@broker.on_event("startup")
async def worker_startup(state):
    """Инициализация ресурсов при старте воркера"""
    setup_logging("worker") 
    logger.info("👷 Worker process starting up...")
    await redis_manager.connect()


@broker.on_event("shutdown")
async def worker_shutdown(state):
    """Закрытие ресурсов при остановке воркера"""
    logger.info("👷 Worker shutting down...")
    await redis_manager.disconnect()
@broker.task(
    task_name="process_pact_messages",
    retry_on_error=True,
    max_retry=3
)
async def process_pact_messages_task(conversation_id: str):
    """
    Production-ready задача обработки диалога:
    Биллинг -> Синхронизация ID -> Сохранение сообщений -> Ответ ИИ
    """
    
    if not redis_manager.redis:
        logger.info(f"🔄 [Worker] Реинициализация Redis для задачи {conversation_id}")
        await redis_manager.connect()

    logger.info(f"--- [Worker] Начало обработки диалога {conversation_id} ---")
    
    async with async_session_maker() as session:
        try:
            new_messages = await redis_manager.get_buffer(conversation_id)
            if not new_messages:
                logger.debug(f"Буфер для {conversation_id} пуст. Выходим.")
                return

            query = select(Dialogue).where(Dialogue.pact_conversation_id == conversation_id)
            result = await session.execute(query)
            dialogue = result.scalar_one_or_none()

            # --- ЛОГИКА ТЕСТОВОГО РЕЖИМА И ТРИГГЕРА ---
            if settings.TEST_MODE:
                trigger_received = any(
                    settings.TEST_TRIGGER.lower() in m.get("text", "").lower() 
                    for m in new_messages
                )
                if trigger_received:
                    if dialogue:
                        await session.delete(dialogue)
                        await session.flush()
                        dialogue = None
                        logger.info(f"🔄 [Test Mode] Старый диалог {conversation_id} удален для ПЕРЕЗАПУСКА.")
                    else:
                        logger.info(f"🚀 [Test Mode] Триггер найден! Создаем новый диалог для {conversation_id}")
                
                elif not dialogue:
                    logger.info(f"🙊 [Test Mode] Диалог {conversation_id} проигнорирован (триггер не найден)")
                    await redis_manager.delete_buffer(conversation_id)
                    return 

            if not dialogue:
                logger.info(f"🆕 Новый клиент. Проверка баланса...")
                stg_query = select(AppSettings).limit(1)
                stg_result = await session.execute(stg_query)
                app_settings = stg_result.scalar_one_or_none()
                
                if not app_settings:
                    logger.error("❌ Критическая ошибка: Таблица AppSettings пуста!")
                    return

                # Стоимость создания диалога из тарифов (поле tariffs: {"dialog_cost": 10.0})
                raw_cost = app_settings.tariffs.get("dialog_cost", 0)
                dialog_cost = Decimal(str(raw_cost))
                billing_update = (
                    update(AppSettings)
                    .where(AppSettings.id == app_settings.id)
                    .where(AppSettings.balance >= dialog_cost)
                    .values(balance=AppSettings.balance - dialog_cost)
                )
                billing_res = await session.execute(billing_update)
                
                if billing_res.rowcount == 0:
                    logger.warning(f"⚠️ Недостаточно средств для диалога {conversation_id}. Обработка прервана.")
                    return # Прекращаем, задача считается выполненной (не ретраим)

                acc_query = select(Account).where(Account.is_active == True).limit(1)
                acc_result = await session.execute(acc_query)
                account = acc_result.scalar_one_or_none()
                if not account:
                    logger.error("❌ Нет активных аккаунтов amoCRM в базе!")
                    raise Exception("Account not found")

                current_balance = app_settings.balance - dialog_cost
                if (current_balance <= app_settings.low_balance_threshold and 
                    not app_settings.is_low_balance_alert_sent):
                    
                    alert_text = (
                        f"⚠️ <b>ВНИМАНИЕ: НИЗКИЙ БАЛАНС</b>\n\n"
                        f"Текущий остаток: <code>{current_balance}</code> руб.\n"
                        f"Пожалуйста, пополните счет для бесперебойной работы бота."
                    )
                    # Отправляем всем пользователям из TELEGRAM_USER_IDS
                    asyncio.create_task(tg_service.send_global_notification(alert_text))
                    app_settings.is_low_balance_alert_sent = True
                
                dialogue = Dialogue(
                    pact_conversation_id=conversation_id,
                    account_id=account.id,
                    current_state="START",
                    history=[]
                )
                session.add(dialogue)
                await session.flush()
                logger.info(f"💰 Баланс списан (-{dialog_cost}). Диалог создан.")

            if not dialogue.amo_lead_id:
                amo_id_from_cache = await redis_manager.redis.get(f"map:{conversation_id}")
                if amo_id_from_cache:
                    dialogue.amo_lead_id = str(amo_id_from_cache)
                    logger.info(f"🔗 Связано с amoCRM Lead ID: {amo_id_from_cache}")
                    await session.commit()

            if dialogue.amo_lead_id and dialogue.status != "completed":
                lead_data = await amo_api.get_lead(dialogue.amo_lead_id)
                if lead_data:
                    pipeline_id = lead_data.get("pipeline_id")
                    allowed_pipelines = settings.ALLOWED_PIPELINES
                    if allowed_pipelines and (pipeline_id not in allowed_pipelines):
                        logger.info(f"🛑 Сделка {dialogue.amo_lead_id} находится в неразрешенной воронке {pipeline_id}. Бот отключается.")
                        dialogue.status = "completed"
                        await session.commit()
                        await redis_manager.delete_buffer(conversation_id)
                        return 

            if dialogue and dialogue.status == "completed":
                logger.info("⏭️ Диалог завершен или отключен. Бот больше не отвечает.")
                await redis_manager.delete_buffer(conversation_id)
                return 

            
            if new_messages:
                extracted = copy.deepcopy(dialogue.extracted_data or {})
                if "received_files" not in extracted: 
                    extracted["received_files"] = []
                
                formatted = []
                for m in new_messages:
                    text_content = m.get("text", "").strip()
                    if settings.TEST_MODE and text_content.lower() == settings.TEST_TRIGGER.lower():
                        continue 
                    msg_id = m.get("message_id") or f"user_{int(time.time() * 1000)}"
                    msk_now = get_msk_time()
                    if text_content: 
                        formatted.append({
                            "role": "user",
                            "content": text_content,
                            "message_id": msg_id,
                            "timestamp_msk": msk_now
                        })

                    for att in m.get("attachments", []):
                        file_name = att.get("file_name", "unknown.file").lower()
                        if file_name.endswith(".pdf"):
                            if file_name not in extracted["received_files"]:
                                extracted["received_files"].append(file_name)
                                current_count = len(extracted["received_files"])
                                formatted.append({
                                    "role": "user", 
                                    "content": f"[SYSTEM COMMAND] Пользователь прислал pdf файл {file_name}. Всего файлов: {current_count}/3",
                                    "message_id": f"sys_{int(time.time() * 1000)}",
                                    "timestamp_msk": get_msk_time()
                                })
                                client_name = extracted.get("name", "Неизвестный")
                                city = extracted.get("city", "—")
                                phone = extracted.get("phone", "—")
                                credit_type = extracted.get("credit_type", "—")
                                detail_parts = []
                                if extracted.get("category"): detail_parts.append(extracted["category"])
                                if extracted.get("market"): detail_parts.append(extracted["market"])
                                details = f" ({', '.join(detail_parts)})" if detail_parts else ""
                                
                                amo_link = None
                                if dialogue.amo_lead_id:
                                    amo_link = f"https://{settings.AMO_SUBDOMAIN}.amocrm.ru/leads/detail/{dialogue.amo_lead_id}"
                                
                                asyncio.create_task(tg_service.send_report_card(
                                    title=f"📄 Новый файл: {file_name}",
                                    fields={
                                        "👤 Клиент": f"{client_name} ({city})",
                                        "📞 Телефон": f"<code>{phone}</code>",
                                        "⚙️ Запрос": f"{credit_type}{details}",
                                        "📊 Прогресс": f"{get_progress_bar(current_count)} ({current_count} из 3)"
                                    },
                                    link=amo_link
                                ))
                                logger.info(f"🔔 [Alert] Задание на отправку уведомления в ТГ создано (файл: {file_name})")

                                if dialogue.amo_lead_id:
                                    pipelines = SETTINGS_DATA.get('amocrm_pipelines', {})
                                    asyncio.create_task(amo_api.update_lead(
                                        lead_id=dialogue.amo_lead_id,
                                        status_id=pipelines.get('status_ai_decision')
                                    ))
                            else:
                                current_count = len(extracted["received_files"])
                                formatted.append({
                                    "role": "user", 
                                    "content": f"[SYSTEM COMMAND] Пользователь прислал ДУБЛИКАТ файла {file_name}. Счетчик файлов не изменился: {current_count}/3",
                                    "message_id": f"sys_dup_{int(time.time() * 1000)}",
                                    "timestamp_msk": get_msk_time()
                                })
                
                updated_history = list(dialogue.history)
                updated_history.extend(formatted)
                dialogue.history = updated_history
                dialogue.extracted_data = extracted 
                flag_modified(dialogue, "extracted_data")
                flag_modified(dialogue, "history")
                dialogue.reminder_level = 0
                dialogue.last_message_at = func.now()
                await session.commit()
                logger.info(f"💾 {len(new_messages)} сообщений (с ID и MSK временем) перенесено в Postgres.")
                await redis_manager.delete_buffer(conversation_id)
            
            else:
                logger.debug("Буфер Redis пуст, продолжаем работу с историей из БД.")

            if dialogue.history and dialogue.history[-1]["role"] == "user":
                logger.info(f"⏳ Генерируем ответ для диалога {conversation_id}...")
                await perform_logic_and_reply(dialogue, session)
                await session.commit()
            else:
                logger.info("⏭️ Ответ не требуется (последнее сообщение не от пользователя).")

            logger.info(f"🏁 Диалог {conversation_id} успешно обработан.")

        except Exception as e:
            await session.rollback()
            logger.error(f"🚨 Ошибка воркера [ID: {conversation_id}]: {str(e)}", exc_info=True)
            alert_msg = (
                f"❌ <b>Критическая ошибка воркера!</b>\n"
                f"Диалог: <code>{conversation_id}</code>\n"
                f"Ошибка: <code>{str(e)}</code>"
            )
            asyncio.create_task(tg_service.send_tech_alert(alert_msg))
            raise e

async def perform_logic_and_reply(dialogue: Dialogue, session):
    """
    Интеграция LangGraph: загрузка стейта -> выполнение графа -> сохранение итогов
    """
    logger.info(f"🤖 Запуск ИИ-логики для диалога {dialogue.pact_conversation_id}")
    llm_history = [
        {"role": msg["role"], "content": msg["content"]} 
        for msg in dialogue.history
    ]
    old_state = dialogue.current_state 
    current_step = dialogue.current_state if dialogue.current_state != "START" else Steps.CONSENT
    
    initial_state: DialogueState = {
        "pact_conversation_id": dialogue.pact_conversation_id,
        "amo_lead_id": dialogue.amo_lead_id,
        "current_step": current_step,
        "messages": llm_history,
        "extracted_data": dialogue.extracted_data or {},
        "files_count": len(dialogue.extracted_data.get("received_files", [])),
        "analysis_result": None,
        "ai_response": None,
        "is_completed": False,
        "stop_factors_found": dialogue.extracted_data.get("stop_factors_found", False),
        "final_destination": dialogue.extracted_data.get("final_destination")
    }

    try:
        final_state = await app_graph.ainvoke(initial_state)
        ai_reply_text = final_state.get("ai_response")
        if not ai_reply_text:
            error_msg = f"⚠️ Граф не сгенерировал ответ для {dialogue.pact_conversation_id} (ai_response is None)"
            logger.error(error_msg)
            asyncio.create_task(tg_service.send_tech_alert(f"🤖 <b>ИИ не выдал ответ!</b>\nДиалог: {dialogue.pact_conversation_id}"))
            return

        success = await pact_api.send_message(
            conversation_id=dialogue.pact_conversation_id,
            message=ai_reply_text
        )

        if success:
            current_extracted = dict(final_state["extracted_data"])
            new_state = final_state["current_step"]
            new_entry = {
                "role": "assistant",
                "state": old_state,                 # В каком стейте пришло сообщение
                "new_state": new_state,             # В какой стейт перешли
                "content": ai_reply_text,
                "message_id": f"bot_{time.time()}", # Генерируем ID для бота
                "timestamp_msk": get_msk_time(),    # Время МСК
                "extracted_data": current_extracted # Срез извлеченных данных
            }
            
            updated_history = list(dialogue.history)
            updated_history.append(new_entry)
            dialogue.history = updated_history
            dialogue.current_state = new_state
            dialogue.extracted_data = current_extracted
            if settings.TEST:
                from src.utils.dialogue_logger import DialogueLogger
                d_logger = DialogueLogger(dialogue.pact_conversation_id)
                d_logger.log_event("ai_reply", ai_reply_text)
                
            if final_state.get("is_completed"):
                dialogue.status = "completed"
                await handle_crm_completion(dialogue, final_state)

            logger.info(f"📤 Ответ отправлен. Шаг изменен на: {dialogue.current_state}")
        else:
            error_text = f"❌ <b>Ошибка отправки в Pact!</b>\nДиалог: {dialogue.pact_conversation_id}\nТекст: {ai_reply_text[:100]}..."
            asyncio.create_task(tg_service.send_tech_alert(error_text))
            
            logger.error(f"❌ Не удалось отправить ответ в Пакт")
            raise Exception("Pact send failed")

    except Exception as e:
        alert_msg = (
            f"🚨 <b>Ошибка в логике ИИ!</b>\n"
            f"Диалог: <code>{dialogue.pact_conversation_id}</code>\n"
            f"Детали: <code>{str(e)}</code>"
        )
        asyncio.create_task(tg_service.send_tech_alert(alert_msg))
        
        logger.error(f"🚨 Ошибка при выполнении графа: {e}", exc_info=True)
        raise e

async def handle_crm_completion(dialogue: Dialogue, final_state: Dict):
    """
    Финальная обработка: перевод в воронки и создание анкеты в amoCRM
    """
    lead_id = dialogue.amo_lead_id
    if not lead_id:
        logger.warning(f"⚠️ Нет amo_lead_id для диалога {dialogue.pact_conversation_id}")
        return

    data = final_state.get('extracted_data', {})
    current_step = final_state.get('current_step')
    dest = final_state.get('final_destination') or data.get('final_destination') or data.get('direction')
    pipelines = SETTINGS_DATA.get('amocrm_pipelines', {})

    target_pipeline = None
    target_status = None
    if dest in ["course"]: 
        target_pipeline = pipelines.get('course_id')
        target_status = pipelines.get('status_course_new')
    elif dest in ["consultation", "consult"]: 
        target_pipeline = pipelines.get('consultation_id')
        target_status = pipelines.get('status_consult_new')
    else:
        target_pipeline = pipelines.get('main_id')
        target_status = pipelines.get('status_ai_decision')
    if target_pipeline and target_status:
        await amo_api.update_lead(
            lead_id=lead_id, 
            pipeline_id=target_pipeline, 
            status_id=target_status
        )
        logger.info(f"🎯 Сделка {lead_id} переведена в воронку {target_pipeline} на статус {target_status}")

    if current_step in [Steps.FINAL_HANDOVER, Steps.CONSULT_FINAL, Steps.CONSULT_INFO]:
        summary = (
            "📋 АНКЕТА ИЗ БОТА:\n"
            f"👤 Имя: {data.get('name', '—')}\n"
            f"📍 Город: {data.get('city', '—')}\n"
            f"📞 Телефон: {data.get('phone', '—')}\n"
            f"💰 Требуемая сумма: {data.get('required_amount') or data.get('car_cost') or '—'}\n"
            f"🏦 Вид кредитования: {data.get('credit_type', '—')}\n"
            f"🏠 Вид залога: {data.get('sub_type', '—')}\n"
            f"📑 Собственник: {'Единственный' if data.get('is_sole_owner') else '—'}\n"
            f"⚠️ Стоп-факторы: {', '.join(data.get('found_factors', [])) if data.get('found_factors') else '—'}"
        )
        
        await amo_api.add_note(lead_id=lead_id, text=summary, note_type="service_message")
        logger.info(f"📝 Анкета для {lead_id} отправлена в amoCRM")
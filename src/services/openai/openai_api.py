import httpx
from openai import AsyncOpenAI
from typing import Type, List, Dict, Any, Optional
from pydantic import BaseModel

from src.core.config import settings
from src.core.logging import logger

class OpenAIService:
    def __init__(self):
        proxy_url = settings.proxy_url
        
        if proxy_url:
            logger.info(f"🌐 OpenAI service is using proxy: {settings.PROXY_HOST}")
            http_client = httpx.AsyncClient(
                proxy=proxy_url,
                timeout=30.0
            )
        else:
            logger.warning("⚠️ OpenAI service is running WITHOUT proxy")
            http_client = httpx.AsyncClient(timeout=30.0)

        # Инициализируем клиент OpenAI
        self.client = AsyncOpenAI(
            api_key=settings.OPENAI_API_KEY,
            http_client=http_client
        )
        self.model = settings.OPENAI_MODEL

    async def analyze_message(
        self, 
        messages: List[Dict[str, str]], 
        response_model: Type[BaseModel],
        system_prompt: str,
        instruction: str
    ) -> Optional[BaseModel]:
        """
        AI-1: Анализатор. 
        Принимает историю и инструкцию, возвращает валидированный Pydantic объект.
        """
        try:
            full_messages = [
                {"role": "system", "content": system_prompt},
                *messages,
                {"role": "user", "content": f"ИНСТРУКЦИЯ ПО АНАЛИЗУ: {instruction}"}
            ]

            completion = await self.client.beta.chat.completions.parse(
                model=self.model,
                messages=full_messages,
                response_format=response_model,
                temperature=0.0
            )

            result = completion.choices[0].message.parsed
            usage = completion.usage
            logger.debug(f"📊 [AI-1 Analyze] Tokens: {usage.total_tokens} | Step: {instruction[:30]}...")
            
            return result

        except Exception as e:
            logger.error(f"❌ Ошибка OpenAI Анализатора: {str(e)}", exc_info=True)
            return None

    async def generate_response(
        self, 
        messages: List[Dict[str, str]], 
        system_prompt: str,
        extra_instruction: str = ""
    ) -> Optional[str]:
        """
        AI-2: Генератор текста.
        Возвращает человечный ответ на основе истории и промпта.
        """
        try:
            full_messages = [
                {"role": "system", "content": system_prompt}
            ]
            if extra_instruction:
                full_messages.append({"role": "system", "content": f"ВАЖНОЕ ТРЕБОВАНИЕ К ОТВЕТУ: {extra_instruction}"})
            
            full_messages.extend(messages)

            completion = await self.client.chat.completions.create(
                model=self.model,
                messages=full_messages,
                temperature=0.7
            )

            response_text = completion.choices[0].message.content
            
            usage = completion.usage
            logger.debug(f"📊 [AI-2 Generate] Tokens: {usage.total_tokens}")
            
            return response_text

        except Exception as e:
            logger.error(f"❌ Ошибка OpenAI Генератора: {str(e)}", exc_info=True)
            return "Извините, произошла техническая ошибка. Пожалуйста, попробуйте позже."

openai_service = OpenAIService()
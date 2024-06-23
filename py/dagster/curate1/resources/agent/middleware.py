import json
from typing import List

from openai import OpenAI
from openai.types.chat.chat_completion_message_param import \
    ChatCompletionMessageParam

from ..llm_response_cache.llm_response_cache import LlmResponseCache


def create_completion(cache: LlmResponseCache, openai: OpenAI, messages: List[ChatCompletionMessageParam], model: str):
  request_str = json.dumps(messages)
  
  cached_response = cache.get_llm_response(request_str, model)
  if cached_response:
    print(f"Got cached response; len={len(cached_response)}")
    return cached_response
  
  response = openai.chat.completions.create(
    messages=messages,
    model=model,
  )

  content = response.choices[0].message.content
  if content:
    cache.insert_llm_response(request_str, model, content)
   
  return content
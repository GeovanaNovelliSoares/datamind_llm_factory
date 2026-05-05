"""LLM client with Groq primary and Ollama fallback."""
from src.config import get_settings
from src.logger import get_logger

logger = get_logger(__name__)


def call_llm(
    prompt: str,
    system: str = "You are a precise data analyst. Respond concisely.",
    temperature: float = 0.1,
    max_tokens: int = 1024,
    json_mode: bool = False,
) -> tuple[str, int, str]:
    """
    Call the configured LLM with automatic fallback.

    Returns: (response_text, tokens_used, provider_name)
    """
    settings = get_settings()

    providers = []
    if settings.llm_provider == "groq" and settings.groq_api_key:
        providers.append(("groq", _call_groq))
    providers.append(("ollama", _call_ollama))

    last_error = None
    for name, fn in providers:
        try:
            text, tokens = fn(prompt, system, temperature, max_tokens, json_mode, settings)
            logger.info("llm_call_success", provider=name, tokens=tokens)
            return text, tokens, name
        except Exception as e:
            logger.warning("llm_provider_failed", provider=name, error=str(e))
            last_error = e

    raise RuntimeError(f"All LLM providers failed. Last: {last_error}") from last_error


def _call_groq(prompt, system, temperature, max_tokens, json_mode, settings) -> tuple[str, int]:
    from groq import Groq
    client = Groq(api_key=settings.groq_api_key)
    kwargs = {}
    if json_mode:
        kwargs["response_format"] = {"type": "json_object"}
    resp = client.chat.completions.create(
        model=settings.llm_model,
        messages=[{"role": "system", "content": system}, {"role": "user", "content": prompt}],
        temperature=temperature,
        max_tokens=max_tokens,
        **kwargs,
    )
    return resp.choices[0].message.content or "", resp.usage.total_tokens if resp.usage else 0


def _call_ollama(prompt, system, temperature, max_tokens, json_mode, settings) -> tuple[str, int]:
    from openai import OpenAI
    client = OpenAI(base_url=f"{settings.ollama_base_url}/v1", api_key="ollama")
    resp = client.chat.completions.create(
        model=settings.ollama_model,
        messages=[{"role": "system", "content": system}, {"role": "user", "content": prompt}],
        temperature=temperature,
        max_tokens=max_tokens,
    )
    return resp.choices[0].message.content or "", resp.usage.total_tokens if resp.usage else 0

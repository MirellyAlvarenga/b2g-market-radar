from unittest.mock import MagicMock, patch

import httpx
import pytest

from ingestion.audit import get_last_watermark
from ingestion.brasilapi_cnpj import fetch_cnpj
from ingestion.pncp_contratacoes import fetch_page_with_retry


def test_fetch_page_returns_json_on_200():
    mock_client = MagicMock(spec=httpx.Client)
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": [{"numeroControlePNCP": "123"}], "paginasRestantes": 0}
    mock_client.get.return_value = mock_response

    result = fetch_page_with_retry(mock_client, {"pagina": 1})

    assert result is not None
    assert len(result["data"]) == 1
    assert result["data"][0]["numeroControlePNCP"] == "123"


def test_fetch_page_returns_none_on_204():
    mock_client = MagicMock(spec=httpx.Client)
    mock_response = MagicMock()
    mock_response.status_code = 204
    mock_client.get.return_value = mock_response

    result = fetch_page_with_retry(mock_client, {"pagina": 2})

    assert result is None


@patch("ingestion.pncp_contratacoes.time.sleep", return_value=None)
def test_fetch_page_retries_on_429_and_succeeds(mock_sleep):
    mock_client = MagicMock(spec=httpx.Client)

    resp_429 = MagicMock()
    resp_429.status_code = 429

    resp_200 = MagicMock()
    resp_200.status_code = 200
    resp_200.json.return_value = {"data": [], "paginasRestantes": 0}

    mock_client.get.side_effect = [resp_429, resp_200]

    result = fetch_page_with_retry(mock_client, {"pagina": 1}, max_retries=3)

    assert result == {"data": [], "paginasRestantes": 0}
    assert mock_client.get.call_count == 2
    mock_sleep.assert_called_once()


def test_fetch_cnpj_handles_404_gracefully():
    mock_client = MagicMock(spec=httpx.Client)
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_client.get.return_value = mock_response

    status, payload = fetch_cnpj(mock_client, "00000000000191")

    assert status == 404
    assert "error" in payload


@patch("ingestion.audit.ensure_audit_table")
def test_get_last_watermark_applies_safety_overlap(mock_ensure):
    mock_engine = MagicMock()
    mock_conn = mock_engine.begin.return_value.__enter__.return_value
    mock_conn.execute.return_value.scalar_one_or_none.return_value = "20261110"

    dt_ini, dt_fim = get_last_watermark(mock_engine, "pncp_contratacoes_bronze", safety_overlap_days=2)

    assert dt_ini == "20261108"
    assert len(dt_fim) == 8
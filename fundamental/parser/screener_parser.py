import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO


class ScreenerParser:

    def parse(self, html: str) -> dict:
        soup = BeautifulSoup(html, "html.parser")

        return {
            "quarterly": self._parse_quarterly(soup),
            "ratios": self._parse_ratios(soup),
            "sector": self._parse_sector(soup),
            "market_cap": self._parse_market_cap(soup),
        }

    # ------------------------------------------------------------------
    # Shared safe numeric conversion (THE MOST IMPORTANT PART)
    # ------------------------------------------------------------------
    def _safe_float(self, value) -> float | None:
        if value is None:
            return None

        text = str(value).strip()

        if text in ("", "-", "--", "NA", "N/A"):
            return None

        text = text.replace(",", "").replace("%", "")

        try:
            return float(text)
        except ValueError:
            return None

    # ------------------------------------------------------------------
    # Quarterly Results
    # ------------------------------------------------------------------
    def _parse_quarterly(self, soup) -> pd.DataFrame:
        table = soup.select_one("section#quarters table")
        if not table:
            raise ValueError("Quarterly results table not found")

        df = pd.read_html(StringIO(str(table)), header=0)[0]
        df.rename(columns={df.columns[0]: "metric"}, inplace=True)

        df["metric"] = (
            df["metric"]
            .astype(str)
            .str.replace("\xa0", " ", regex=False)
            .str.replace("+", "", regex=False)
            .str.strip()
        )

        # Normalize all numeric cells
        for col in df.columns[1:]:
            df[col] = df[col].apply(self._safe_float)

        return df

    # ------------------------------------------------------------------
    # Top Ratios (ROE, ROCE, EPS, etc.)
    # ------------------------------------------------------------------
    def _parse_ratios(self, soup) -> pd.DataFrame:
        rows = []

        for li in soup.select("ul#top-ratios li"):
            name = li.select_one("span.name")
            value = li.select_one("span.number")

            if not name:
                continue

            rows.append({
                "Metric": name.text.strip(),
                "Value": self._safe_float(value.text if value else None)
            })

        return pd.DataFrame(rows, columns=["Metric", "Value"])

    # ------------------------------------------------------------------
    # Sector / Industry Hierarchy
    # ------------------------------------------------------------------
    def _parse_sector(self, soup) -> pd.DataFrame:
        peer_section = soup.select_one("section#peers p.sub")
        if not peer_section:
            return pd.DataFrame(columns=["Category", "Value"])

        links = peer_section.find_all("a")
        keys = ["Broad Sector", "Sector", "Industry Group", "Industry"]
        values = [a.text.strip() for a in links[:4]]

        return pd.DataFrame(zip(keys, values), columns=["Category", "Value"])

    # ------------------------------------------------------------------
    # Market Cap (FIXED)
    # ------------------------------------------------------------------
    def _parse_market_cap(self, soup) -> float | None:
        for li in soup.select("ul#top-ratios li"):
            name = li.select_one("span.name")
            value = li.select_one("span.number")

            if not name or not value:
                continue

            if name.text.strip() == "Market Cap":
                return self._safe_float(value.text)

        return None

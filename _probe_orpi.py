"""Quick DOM probe for Orpi — find the right selector pattern."""
import asyncio
import re
from playwright.async_api import async_playwright


URL = (
    "https://www.orpi.com/recherche/buy?action=transaction"
    "&typeTransaction=location&typeBien%5B0%5D=appartement"
    "&prixMax=1100&codeInsee%5B0%5D=75056"
)


async def main():
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            locale="fr-FR",
            viewport={"width": 1280, "height": 900},
        )
        page = await ctx.new_page()
        print(f"Going to: {URL}")
        resp = await page.goto(URL, wait_until="domcontentloaded", timeout=45000)
        print(f"Status: {resp.status if resp else 'N/A'}")
        # Cookie banner
        for txt in ("Accepter", "Tout accepter", "OK", "J'accepte"):
            try:
                btn = page.get_by_role("button", name=re.compile(txt, re.I))
                if await btn.count():
                    await btn.first.click(timeout=2500)
                    print(f"Clicked banner: {txt}")
                    break
            except Exception:
                pass
        try:
            await page.wait_for_selector("article", state="attached", timeout=20000)
        except Exception:
            print("wait_for_selector article: timeout")
        await asyncio.sleep(5)
        html = await page.content()
        print(f"HTML size: {len(html)}")

        # Find anchors that look like listing detail links
        hrefs = re.findall(r'href="(/annonce[^"]+)"', html)
        print(f"/annonce hrefs: {len(hrefs)} (first 5: {hrefs[:5]})")
        hrefs2 = re.findall(r'href="(/[a-z0-9/-]*location[^"]*-\d+)"', html, re.I)
        print(f"location-N hrefs: {len(hrefs2)} (first 5: {hrefs2[:5]})")
        hrefs3 = re.findall(r'href="(https?://www\.orpi\.com/[^"]+/\d+)"', html)
        print(f"orpi.com/...id hrefs: {len(hrefs3)} (first 5: {hrefs3[:5]})")
        # Generic — any orpi.com listing-shaped href
        all_hrefs = re.findall(r'href="(/[^"]+)"', html)
        # filter: contain a digit segment at end that's >=5 digits
        candidate = [h for h in all_hrefs if re.search(r"/\d{5,}", h) and "annonce" in h.lower()]
        print(f"candidates: {len(candidate)} (first 5: {candidate[:5]})")

        # Sample articles
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        arts = soup.find_all("article")
        print(f"<article> count: {len(arts)}")
        if arts:
            # Dump the full first article so we can find price/title/url/surface
            print("---- first <article> FULL ----")
            print(str(arts[0]))
            print("---- end ----")
        # Look for class names containing 'card'
        cards = soup.find_all(class_=re.compile(r"card", re.I))
        print(f"class=*card* count: {len(cards)}")
        if cards and not arts:
            print("---- first .card snippet ----")
            print(str(cards[0])[:1500])

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())

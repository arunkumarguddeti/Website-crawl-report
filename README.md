# Crawl-report
## Adding a new site

1. Copy `.github/workflows/crawl-site4.yml` → rename to `crawl-site5.yml`
2. Inside the new file, change these 4 things:
   - `name:` at the top → `Broken Link Crawler — Site 5`
   - Dedup key: `site4` → `site5` (2 occurrences)
   - Cron times → stagger 30 min later than the previous site
   - `secrets.BASE_URL_SITE4` → `secrets.BASE_URL_SITE5`
   - Archive path: `archive/site4` → `archive/site5` (3 occurrences)
3. Add secret `BASE_URL_SITE5` in repo Settings → Secrets → Actions
4. Commit and push — the new workflow appears in the Actions tab immediately

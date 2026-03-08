import express from "express";
import fetch from "node-fetch";
import "dotenv/config";
import fs from "fs";
import cors from "cors";
import { Redis } from "@upstash/redis";

const app = express();
const PORT = process.env.PORT || 3000;

// ----------------------------
// 필수 환경변수
// ----------------------------
const YT_API_KEY = process.env.YT_API_KEY;
const redis = Redis.fromEnv();

// ----------------------------
// CORS
// ----------------------------
// 지금은 PC 버전 포함해서 모두 허용
// 나중에 필요하면 origin 제한 가능
app.use(cors());

// ----------------------------
// 정책
// ----------------------------
const MIN_DURATION_SEC = 300; // 5분 이상만
const EXCLUDE_LIVE = true; // 라이브 / 예정 제외
const REGION_CODE = "KR";
const SAFE_SEARCH = "moderate";

const MAX_CHANNELS = 10;
const DEDUPE_DAYS = 3;

const CHANNELS_FILE = "./channels.json";

// Redis 키
const TODAY_CACHE_KEY = "tvapp:daily-cache";
const LAST_GOOD_CACHE_KEY = "tvapp:last-good-cache";
const HISTORY_KEY = "tvapp:history";
const BUILD_LOCK_KEY = "tvapp:build-lock";

// 쿼터 관리
const DAILY_QUOTA_LIMIT = 10000;
const DAILY_QUOTA_SAFE_LIMIT = 9000;
const SEARCH_COST = 100;
const VIDEOS_COST = 1;

// 락 / 대기
const BUILD_LOCK_TTL_SEC = 300; // 5분
const WAIT_FOR_CACHE_MS = 60000; // 최대 60초
const WAIT_POLL_MS = 2000; // 2초마다 확인

// ----------------------------
// 유틸
// ----------------------------
function readJson(path, fallback) {
  try {
    return JSON.parse(fs.readFileSync(path, "utf-8"));
  } catch {
    return fallback;
  }
}

function isoDurationToSeconds(iso) {
  if (!iso) return 0;
  const m = iso.match(/PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?/);
  if (!m) return 0;
  const h = parseInt(m[1] || "0", 10);
  const min = parseInt(m[2] || "0", 10);
  const s = parseInt(m[3] || "0", 10);
  return h * 3600 + min * 60 + s;
}

function todayKey() {
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function recentDayKeys(n) {
  const keys = [];
  for (let i = 1; i <= n; i++) {
    const d = new Date();
    d.setDate(d.getDate() - i);
    const yyyy = d.getFullYear();
    const mm = String(d.getMonth() + 1).padStart(2, "0");
    const dd = String(d.getDate()).padStart(2, "0");
    keys.push(`${yyyy}-${mm}-${dd}`);
  }
  return keys;
}

function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) {
    out.push(arr.slice(i, i + size));
  }
  return out;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function getPerKeywordTarget(keywordCount) {
  if (keywordCount <= 1) return 300;
  if (keywordCount === 2) return 150;
  return 100;
}

function hasUsableChannels(payload) {
  return (
    payload &&
    Array.isArray(payload.channels) &&
    payload.channels.length > 0 &&
    payload.channels.some(
      (ch) => Array.isArray(ch.videos) && ch.videos.length > 0
    )
  );
}

// ----------------------------
// Redis helpers
// ----------------------------
async function getTodayCache() {
  const data = await redis.get(TODAY_CACHE_KEY);
  return data || null;
}

async function setTodayCache(payload) {
  await redis.set(TODAY_CACHE_KEY, payload);
}

async function getLastGoodCache() {
  const data = await redis.get(LAST_GOOD_CACHE_KEY);
  return data || null;
}

async function setLastGoodCache(payload) {
  await redis.set(LAST_GOOD_CACHE_KEY, payload);
}

async function getHistory() {
  const data = await redis.get(HISTORY_KEY);
  return data || {};
}

async function setHistory(history) {
  await redis.set(HISTORY_KEY, history);
}

function getQuotaKey(dayKey) {
  return `tvapp:quota:${dayKey}`;
}

async function getQuotaUsed(dayKey) {
  const value = await redis.get(getQuotaKey(dayKey));
  return Number(value || 0);
}

async function addQuotaUsed(dayKey, amount) {
  const key = getQuotaKey(dayKey);
  const next = await redis.incrby(key, amount);
  await redis.expire(key, 60 * 60 * 24 * 3);
  return Number(next);
}

async function tryAcquireBuildLock() {
  const result = await redis.set(BUILD_LOCK_KEY, "1", {
    nx: true,
    ex: BUILD_LOCK_TTL_SEC,
  });
  return result === "OK";
}

async function releaseBuildLock() {
  await redis.del(BUILD_LOCK_KEY);
}

async function isBuildLocked() {
  const value = await redis.get(BUILD_LOCK_KEY);
  return value !== null;
}

// ----------------------------
// 쿼터 보호
// ----------------------------
async function ensureQuotaAvailable(cost) {
  const day = todayKey();
  const used = await getQuotaUsed(day);

  if (used + cost > DAILY_QUOTA_SAFE_LIMIT) {
    throw new Error(
      `Quota guard blocked request. used=${used}, nextCost=${cost}, safeLimit=${DAILY_QUOTA_SAFE_LIMIT}, hardLimit=${DAILY_QUOTA_LIMIT}`
    );
  }

  await addQuotaUsed(day, cost);
}

// ----------------------------
// YouTube API
// ----------------------------
async function searchVideoIdsByPhrase(keywordPhrase, wantCount) {
  const q = String(keywordPhrase || "").trim();
  if (!q) return [];

  let collected = [];
  let nextPageToken = "";
  let remaining = Math.max(0, wantCount);

  while (remaining > 0) {
    const batchSize = Math.min(50, remaining);

    await ensureQuotaAvailable(SEARCH_COST);

    const url =
      `https://www.googleapis.com/youtube/v3/search?` +
      `part=snippet&type=video&maxResults=${batchSize}` +
      `&q=${encodeURIComponent(q)}` +
      `&regionCode=${REGION_CODE}` +
      `&safeSearch=${SAFE_SEARCH}` +
      (nextPageToken ? `&pageToken=${encodeURIComponent(nextPageToken)}` : "") +
      `&key=${YT_API_KEY}`;

    const r = await fetch(url);
    const j = await r.json();

    if (!r.ok) {
      throw new Error(`YouTube search.list failed: ${JSON.stringify(j)}`);
    }

    const ids = (j.items || [])
      .map((it) => it.id?.videoId)
      .filter(Boolean);

    collected.push(...ids);

    nextPageToken = j.nextPageToken || "";
    remaining -= ids.length;

    if (!nextPageToken || ids.length === 0) {
      break;
    }
  }

  return [...new Set(collected)];
}

async function fetchVideoDetails(videoIds) {
  if (!videoIds.length) return [];

  const chunks = chunkArray(videoIds, 50);
  const all = [];

  for (const ids of chunks) {
    await ensureQuotaAvailable(VIDEOS_COST);

    const url =
      `https://www.googleapis.com/youtube/v3/videos?` +
      `part=contentDetails,snippet&id=${ids.join(",")}` +
      `&key=${YT_API_KEY}`;

    const r = await fetch(url);
    const j = await r.json();

    if (!r.ok) {
      throw new Error(`YouTube videos.list failed: ${JSON.stringify(j)}`);
    }

    const mapped = (j.items || []).map((it) => {
      const durationSec = isoDurationToSeconds(it.contentDetails?.duration);
      const live = it.snippet?.liveBroadcastContent;

      return {
        videoId: it.id,
        title: it.snippet?.title,
        thumb: it.snippet?.thumbnails?.medium?.url,
        publishedAt: it.snippet?.publishedAt,
        durationSec,
        liveBroadcastContent: live,
      };
    });

    all.push(...mapped);
  }

  return all;
}

// ----------------------------
// 재생목록 조립 규칙
// ----------------------------
// fallback 먼저 유지
// fresh는 맨 뒤에 붙임
// fresh 개수만큼 fallback 앞부분 삭제
function mergeVideosWithTailFresh(fresh, fallback, maxVideos) {
  const freshSelected = fresh.slice(0, maxVideos);

  if (freshSelected.length >= maxVideos) {
    return freshSelected.slice(0, maxVideos);
  }

  const fallbackBase = fallback.slice(0, maxVideos);
  const cutCount = freshSelected.length;
  const fallbackTrimmed = fallbackBase.slice(cutCount);
  const merged = [...fallbackTrimmed, ...freshSelected];

  return merged.slice(0, maxVideos);
}

// ----------------------------
// 채널 빌드
// ----------------------------
async function buildOneChannel(ch, seenIdsSet) {
  const name = ch?.name || "채널";
  const keywords = Array.isArray(ch?.keywords)
    ? ch.keywords.map((k) => String(k).trim()).filter(Boolean)
    : [];

  const maxVideos = typeof ch?.maxVideos === "number" ? ch.maxVideos : 100;

  const keywordCount = keywords.length;
  const perKeywordTarget = getPerKeywordTarget(keywordCount);

  let collectedIds = [];
  for (const keywordPhrase of keywords) {
    const ids = await searchVideoIdsByPhrase(keywordPhrase, perKeywordTarget);
    collectedIds.push(...ids);
  }

  const uniqueIds = [...new Set(collectedIds)];
  const details = await fetchVideoDetails(uniqueIds);

  let candidates = details.filter((v) => (v.durationSec || 0) >= MIN_DURATION_SEC);

  if (EXCLUDE_LIVE) {
    candidates = candidates.filter((v) => v.liveBroadcastContent === "none");
  }

  candidates.sort((a, b) => String(b.publishedAt).localeCompare(String(a.publishedAt)));

  const fresh = candidates.filter((v) => !seenIdsSet.has(v.videoId));
  const fallback = candidates.filter((v) => seenIdsSet.has(v.videoId));

  const picked = mergeVideosWithTailFresh(fresh, fallback, maxVideos);

  return {
    name,
    keywords,
    requestedPerKeyword: perKeywordTarget,
    requestedTotalCandidates: perKeywordTarget * keywordCount,
    uniqueCandidateCount: uniqueIds.length,
    freshCount: fresh.length,
    fallbackCount: fallback.length,
    finalVideoCount: picked.length,
    videos: picked.map((v) => ({
      videoId: v.videoId,
      title: v.title,
      thumb: v.thumb,
      publishedAt: v.publishedAt,
      durationSec: v.durationSec,
    })),
  };
}

async function buildSchedule() {
  const channelsConfig = readJson(CHANNELS_FILE, []);
  const pickedConfig = (Array.isArray(channelsConfig) ? channelsConfig : []).slice(0, MAX_CHANNELS);

  const history = await getHistory();
  const keys = recentDayKeys(DEDUPE_DAYS);

  const seenIds = new Set();
  for (const day of keys) {
    const dayObj = history?.[day];
    if (!dayObj) continue;

    for (const channelName of Object.keys(dayObj)) {
      const arr = dayObj[channelName];
      if (Array.isArray(arr)) {
        for (const id of arr) seenIds.add(id);
      }
    }
  }

  const channels = [];
  for (const ch of pickedConfig) {
    const built = await buildOneChannel(ch, seenIds);
    channels.push(built);
  }

  const payload = {
    updatedAt: new Date().toISOString(),
    dayKey: todayKey(),
    channelCount: channels.length,
    quotaUsedByServerToday: await getQuotaUsed(todayKey()),
    channels,
  };

  // 성공한 재생목록일 때만 history 갱신
  if (hasUsableChannels(payload)) {
    const today = todayKey();
    history[today] = history[today] || {};

    for (const ch of payload.channels) {
      history[today][ch.name] = (ch.videos || [])
        .map((v) => v.videoId)
        .filter(Boolean);
    }

    const keepDays = 30;
    const allDays = Object.keys(history).sort();
    const cutoffIndex = Math.max(0, allDays.length - keepDays);
    for (let i = 0; i < cutoffIndex; i++) {
      delete history[allDays[i]];
    }

    await setHistory(history);
  }

  return payload;
}

// ----------------------------
// 중복 생성 방지
// ----------------------------
async function waitForExistingBuildResult() {
  const start = Date.now();

  while (Date.now() - start < WAIT_FOR_CACHE_MS) {
    const cached = await getTodayCache();

    if (
      cached?.dayKey === todayKey() &&
      Array.isArray(cached?.channels) &&
      cached.channels.length > 0
    ) {
      return cached;
    }

    const locked = await isBuildLocked();
    if (!locked) {
      return null;
    }

    await sleep(WAIT_POLL_MS);
  }

  return null;
}

async function buildAndSaveOnlyIfUsable() {
  const fresh = await buildSchedule();

  // 1순위: 기존 성공 캐시 보호
  // 2순위: 빈 재생목록 저장 금지
  if (!hasUsableChannels(fresh)) {
    throw new Error("Generated schedule is empty. Refusing to overwrite existing good cache.");
  }

  await setTodayCache(fresh);
  await setLastGoodCache(fresh);

  return fresh;
}

async function getBestAvailableCache() {
  const todayCache = await getTodayCache();
  if (
    todayCache?.dayKey === todayKey() &&
    Array.isArray(todayCache?.channels) &&
    todayCache.channels.length > 0
  ) {
    return {
      source: "today",
      payload: todayCache,
    };
  }

  const lastGood = await getLastGoodCache();
  if (
    lastGood &&
    Array.isArray(lastGood?.channels) &&
    lastGood.channels.length > 0
  ) {
    return {
      source: "lastGood",
      payload: lastGood,
    };
  }

  return {
    source: null,
    payload: null,
  };
}

// ----------------------------
// 라우트
// ----------------------------
app.get("/", (req, res) => {
  res.send("OK");
});

app.get("/daykey", async (req, res) => {
  try {
    const todayCache = await getTodayCache();
    const lastGood = await getLastGoodCache();
    const quotaUsed = await getQuotaUsed(todayKey());

    return res.json({
      today: todayKey(),
      hasTodayCache:
        todayCache?.dayKey === todayKey() &&
        Array.isArray(todayCache?.channels) &&
        todayCache.channels.length > 0,
      hasLastGoodCache:
        !!lastGood &&
        Array.isArray(lastGood?.channels) &&
        lastGood.channels.length > 0,
      todayCacheDayKey: todayCache?.dayKey ?? null,
      lastGoodDayKey: lastGood?.dayKey ?? null,
      quotaUsedByServerToday: quotaUsed,
      quotaSafeLimit: DAILY_QUOTA_SAFE_LIMIT,
      quotaHardLimit: DAILY_QUOTA_LIMIT,
      buildLocked: await isBuildLocked(),
    });
  } catch (e) {
    return res.status(500).json({ error: String(e) });
  }
});

app.get("/channels", async (req, res) => {
  try {
    if (!YT_API_KEY) {
      return res.status(500).json({
        error: "YT_API_KEY is not set (Render Environment 확인)",
      });
    }

    // 먼저 가장 좋은 캐시를 찾음
    const best = await getBestAvailableCache();

    // 오늘 캐시가 있으면 무조건 사용
    if (best.source === "today") {
      return res.json({
        ...best.payload,
        cached: true,
        cacheSource: "today",
      });
    }

    // 이미 누군가 생성 중이면 기다림
    const locked = await isBuildLocked();
    if (locked) {
      const waited = await waitForExistingBuildResult();

      if (
        waited?.dayKey === todayKey() &&
        Array.isArray(waited?.channels) &&
        waited.channels.length > 0
      ) {
        return res.json({
          ...waited,
          cached: true,
          cacheSource: "today",
          waitedForBuild: true,
        });
      }

      // 기다려도 오늘 캐시가 안 생기면 lastGood라도 줌
      if (best.source === "lastGood") {
        return res.json({
          ...best.payload,
          cached: true,
          stale: true,
          cacheSource: "lastGood",
          staleReason: "Build in progress, using previous successful playlist.",
        });
      }

      return res.status(503).json({
        error: "Schedule is being built. Please try again shortly.",
      });
    }

    // 락 획득 시도
    const acquired = await tryAcquireBuildLock();
    if (!acquired) {
      const waited = await waitForExistingBuildResult();

      if (
        waited?.dayKey === todayKey() &&
        Array.isArray(waited?.channels) &&
        waited.channels.length > 0
      ) {
        return res.json({
          ...waited,
          cached: true,
          cacheSource: "today",
          waitedForBuild: true,
        });
      }

      if (best.source === "lastGood") {
        return res.json({
          ...best.payload,
          cached: true,
          stale: true,
          cacheSource: "lastGood",
          staleReason: "Lock contention, using previous successful playlist.",
        });
      }

      return res.status(503).json({
        error: "Schedule build lock contention. Please try again shortly.",
      });
    }

    try {
      // 3순위: quota 부족 시 새 생성 중단 + 기존 캐시 반환
      const fresh = await buildAndSaveOnlyIfUsable();

      return res.json({
        ...fresh,
        cached: false,
        cacheSource: "newlyBuilt",
      });
    } catch (e) {
      const fallback = await getLastGoodCache();

      if (
        fallback &&
        Array.isArray(fallback?.channels) &&
        fallback.channels.length > 0
      ) {
        return res.json({
          ...fallback,
          cached: true,
          stale: true,
          cacheSource: "lastGood",
          staleReason: String(e),
        });
      }

      return res.status(500).json({ error: String(e) });
    } finally {
      await releaseBuildLock();
    }
  } catch (e) {
    const fallback = await getLastGoodCache();

    if (
      fallback &&
      Array.isArray(fallback?.channels) &&
      fallback.channels.length > 0
    ) {
      return res.json({
        ...fallback,
        cached: true,
        stale: true,
        cacheSource: "lastGood",
        staleReason: String(e),
      });
    }

    return res.status(500).json({ error: String(e) });
  }
});

app.listen(PORT, () => {
  console.log(`Server running on ${PORT}`);
});
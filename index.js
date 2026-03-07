import express from "express";
import fetch from "node-fetch";
import "dotenv/config";
import fs from "fs";
import { Redis } from "@upstash/redis";

const app = express();
const PORT = process.env.PORT || 3000;

// ----------------------------
// 필수 환경변수
// ----------------------------
const YT_API_KEY = process.env.YT_API_KEY;
const redis = Redis.fromEnv();

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
const CACHE_KEY = "tvapp:daily-cache";
const HISTORY_KEY = "tvapp:history";
const BUILD_LOCK_KEY = "tvapp:build-lock";

// 쿼터 관리
const DAILY_QUOTA_LIMIT = 10000;
const DAILY_QUOTA_SAFE_LIMIT = 9000; // 안전한 상한선
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

// ----------------------------
// Redis helpers
// ----------------------------
async function getCache() {
  const data = await redis.get(CACHE_KEY);
  return data || null;
}

async function setCache(payload) {
  await redis.set(CACHE_KEY, payload);
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

  // 3일 정도 유지
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

    // search.list 비용 100
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
    // videos.list 비용 1
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
// 새 재생목록 조립 규칙
// ----------------------------
// 기존 규칙:
// fresh 먼저, 부족하면 fallback
//
// 새 규칙:
// fallback(중복 영상) 먼저 유지
// fresh(새 영상)는 맨 뒤에 붙임
// fresh 개수만큼 fallback 맨 앞을 잘라냄
//
// 예)
// maxVideos = 300
// fallback = 300개
// fresh = 40개
// 결과 = fallback 뒤 260개 + fresh 40개
//
function mergeVideosWithTailFresh(fresh, fallback, maxVideos) {
  const freshSelected = fresh.slice(0, maxVideos);

  if (freshSelected.length >= maxVideos) {
    // 새 영상만으로도 꽉 차면 그것만 사용
    return freshSelected.slice(0, maxVideos);
  }

  // fallback은 최대 maxVideos만 먼저 확보
  const fallbackBase = fallback.slice(0, maxVideos);

  // fresh를 뒤에 붙이기 위해, 그 개수만큼 앞에서 잘라냄
  const cutCount = freshSelected.length;
  const fallbackTrimmed = fallbackBase.slice(cutCount);

  const merged = [...fallbackTrimmed, ...freshSelected];

  // 혹시 fallback이 부족하면 maxVideos보다 짧을 수 있음
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

  // 최근 3일에 없던 영상
  const fresh = candidates.filter((v) => !seenIdsSet.has(v.videoId));

  // 최근 3일에 있던 영상
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

  // 오늘 사용한 videoId 기록
  const today = todayKey();
  history[today] = history[today] || {};

  for (const ch of payload.channels) {
    history[today][ch.name] = (ch.videos || []).map((v) => v.videoId).filter(Boolean);
  }

  // 최근 30일만 유지
  const keepDays = 30;
  const allDays = Object.keys(history).sort();
  const cutoffIndex = Math.max(0, allDays.length - keepDays);
  for (let i = 0; i < cutoffIndex; i++) {
    delete history[allDays[i]];
  }

  await setHistory(history);

  return payload;
}

// ----------------------------
// 중복 생성 방지
// ----------------------------
async function waitForExistingBuildResult() {
  const start = Date.now();

  while (Date.now() - start < WAIT_FOR_CACHE_MS) {
    const cached = await getCache();

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

// ----------------------------
// 라우트
// ----------------------------
app.get("/", (req, res) => {
  res.send("OK");
});

app.get("/daykey", async (req, res) => {
  try {
    const cached = await getCache();
    const quotaUsed = await getQuotaUsed(todayKey());

    return res.json({
      today: todayKey(),
      cachedDayKey: cached?.dayKey ?? null,
      hasTodayCache: cached?.dayKey === todayKey(),
      hasChannels: Array.isArray(cached?.channels) && cached.channels.length > 0,
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

    // 1) 오늘 캐시가 있으면 무조건 사용
    const cached = await getCache();
    if (
      cached?.dayKey === todayKey() &&
      Array.isArray(cached?.channels) &&
      cached.channels.length > 0
    ) {
      return res.json({ ...cached, cached: true });
    }

    // 2) 이미 누군가 생성 중이면 기다림
    const locked = await isBuildLocked();
    if (locked) {
      const waited = await waitForExistingBuildResult();

      if (
        waited?.dayKey === todayKey() &&
        Array.isArray(waited?.channels) &&
        waited.channels.length > 0
      ) {
        return res.json({ ...waited, cached: true, waitedForBuild: true });
      }

      return res.status(503).json({
        error: "Schedule is being built. Please try again shortly.",
      });
    }

    // 3) 락 획득
    const acquired = await tryAcquireBuildLock();
    if (!acquired) {
      const waited = await waitForExistingBuildResult();

      if (
        waited?.dayKey === todayKey() &&
        Array.isArray(waited?.channels) &&
        waited.channels.length > 0
      ) {
        return res.json({ ...waited, cached: true, waitedForBuild: true });
      }

      return res.status(503).json({
        error: "Schedule build lock contention. Please try again shortly.",
      });
    }

    try {
      // 4) 오늘 캐시가 없을 때만 생성
      const fresh = await buildSchedule();
      await setCache(fresh);

      return res.json({ ...fresh, cached: false });
    } finally {
      await releaseBuildLock();
    }
  } catch (e) {
    return res.status(500).json({ error: String(e) });
  }
});

app.listen(PORT, () => {
  console.log(`Server running on ${PORT}`);
});
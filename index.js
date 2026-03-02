import express from "express";
import fetch from "node-fetch";
import "dotenv/config";
import fs from "fs";

const app = express();
const PORT = process.env.PORT || 3000;

// ----------------------------
// 필수 환경변수
// ----------------------------
const YT_API_KEY = process.env.YT_API_KEY;

// ----------------------------
// 정책(원하면 여기만 바꾸면 됨)
// ----------------------------
const MIN_DURATION_SEC = 300; // 5분 이상만
const EXCLUDE_LIVE = true; // 라이브/예정 제외
const REGION_CODE = "KR";
const SAFE_SEARCH = "moderate";

// 채널 최대 개수 (너는 10개)
const MAX_CHANNELS = 10;

// 검색 후보 개수(YouTube search는 최대 50)
// 50으로 해도 되는데, "케이블예능/스포츠"처럼 결과가 적을 때를 위해 50 추천
const SEARCH_CANDIDATES = 50;

// "최근 N일" 중복 방지 (A = 3일)
const DEDUPE_DAYS = 3;

// 파일들
const CHANNELS_FILE = "./channels.json"; // ✅ 채널/키워드 설정
const CACHE_FILE = "./cache.json"; // 오늘 편성표 저장
const HISTORY_FILE = "./history.json"; // 최근 사용 videoId 기록

// ----------------------------
// 유틸
// ----------------------------
function readJson(path, fallback) {
  try {
    return JSON.parse(fs.readFileSync(path, "utf-8"));
  } catch (e) {
    return fallback;
  }
}

function writeJson(path, data) {
  fs.writeFileSync(path, JSON.stringify(data, null, 2), "utf-8");
}

// ISO 8601 duration (PT#H#M#S) -> seconds
function isoDurationToSeconds(iso) {
  if (!iso) return 0;
  const m = iso.match(/PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?/);
  if (!m) return 0;
  const h = parseInt(m[1] || "0", 10);
  const min = parseInt(m[2] || "0", 10);
  const s = parseInt(m[3] || "0", 10);
  return h * 3600 + min * 60 + s;
}

// 오늘 날짜키(서버 기준) "YYYY-MM-DD"
function todayKey() {
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

// 최근 N일(어제부터 N일) 날짜키 배열
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

// ----------------------------
// YouTube API
// ----------------------------

// search.list: 키워드들을 " "로 합쳐서 한 번에 검색 (max 50)
async function searchVideoIdsByKeywords(keywords, wantCount) {
  const q = (keywords || []).join(" ").trim();
  if (!q) return [];

  const want = Math.min(Math.max(wantCount || 30, 1), 50);

  const url =
    `https://www.googleapis.com/youtube/v3/search?` +
    `part=snippet&type=video&maxResults=${want}` +
    `&q=${encodeURIComponent(q)}` +
    `&regionCode=${REGION_CODE}` +
    `&safeSearch=${SAFE_SEARCH}` +
    `&key=${YT_API_KEY}`;

  const r = await fetch(url);
  const j = await r.json();

  const ids = (j.items || [])
    .map((it) => it.id?.videoId)
    .filter(Boolean);

  return [...new Set(ids)];
}

// videos.list: 길이/라이브 여부 포함 상세 (최대 50개 묶어서)
async function fetchVideoDetails(videoIds) {
  if (!videoIds.length) return [];

  const url =
    `https://www.googleapis.com/youtube/v3/videos?` +
    `part=contentDetails,snippet&id=${videoIds.join(",")}` +
    `&key=${YT_API_KEY}`;

  const r = await fetch(url);
  const j = await r.json();

  return (j.items || []).map((it) => {
    const durationSec = isoDurationToSeconds(it.contentDetails?.duration);
    const live = it.snippet?.liveBroadcastContent; // "live" | "upcoming" | "none"

    return {
      videoId: it.id,
      title: it.snippet?.title,
      thumb: it.snippet?.thumbnails?.medium?.url,
      publishedAt: it.snippet?.publishedAt,
      durationSec,
      liveBroadcastContent: live,
    };
  });
}

// 한 채널 만들기 (✅ 최근 3일 중복 방지 포함)
async function buildOneChannel(ch, seenIdsSet) {
  const name = ch?.name || "채널";
  const keywords = Array.isArray(ch?.keywords) ? ch.keywords : [];
  const maxVideos = typeof ch?.maxVideos === "number" ? ch.maxVideos : 30;

  // 후보는 SEARCH_CANDIDATES(최대 50)
  const ids = await searchVideoIdsByKeywords(keywords, SEARCH_CANDIDATES);
  const details = await fetchVideoDetails(ids);

  // 1) 5분 이상
  let candidates = details.filter((v) => (v.durationSec || 0) >= MIN_DURATION_SEC);

  // 2) 라이브/예정 제외
  if (EXCLUDE_LIVE) {
    candidates = candidates.filter((v) => v.liveBroadcastContent === "none");
  }

  // 3) 최신순
  candidates.sort((a, b) => String(b.publishedAt).localeCompare(String(a.publishedAt)));

  // ✅ 4) 최근 3일에 나온 적 없는 영상 먼저
  const fresh = candidates.filter((v) => !seenIdsSet.has(v.videoId));

  // ✅ 5) 부족하면 겹치는 영상으로 채움
  const fallback = candidates.filter((v) => seenIdsSet.has(v.videoId));

  let picked = fresh.slice(0, maxVideos);
  if (picked.length < maxVideos) {
    const need = maxVideos - picked.length;
    picked = picked.concat(fallback.slice(0, need));
  }

  return {
    name,
    keywords,
    videos: picked.map((v) => ({
      videoId: v.videoId,
      title: v.title,
      thumb: v.thumb,
      publishedAt: v.publishedAt,
      durationSec: v.durationSec,
    })),
  };
}

// 전체 편성표 만들기(하루 1번) + history 기록
async function buildScheduleOncePerDay() {
  const channelsConfig = readJson(CHANNELS_FILE, []);
  const pickedConfig = (Array.isArray(channelsConfig) ? channelsConfig : []).slice(0, MAX_CHANNELS);

  // ✅ 최근 3일 history에서 seen videoIds 모으기
  const history = readJson(HISTORY_FILE, {});
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

  // ✅ 채널 만들기
  const channels = [];
  for (const ch of pickedConfig) {
    channels.push(await buildOneChannel(ch, seenIds));
  }

  const payload = {
    updatedAt: new Date().toISOString(),
    dayKey: todayKey(),
    channels,
  };

  // ✅ 오늘 사용한 videoId를 history에 기록
  const today = todayKey();
  history[today] = history[today] || {};

  for (const ch of payload.channels) {
    history[today][ch.name] = (ch.videos || []).map((v) => v.videoId).filter(Boolean);
  }

  // ✅ history가 너무 커지는 걸 방지: 최근 30일만 유지
  const keepDays = 30;
  const allDays = Object.keys(history).sort();
  const cutoffIndex = Math.max(0, allDays.length - keepDays);
  for (let i = 0; i < cutoffIndex; i++) {
    delete history[allDays[i]];
  }

  writeJson(HISTORY_FILE, history);

  return payload;
}

// ----------------------------
// 라우트
// ----------------------------
app.get("/", (req, res) => res.send("OK"));

app.get("/channels", async (req, res) => {
  try {
    if (!YT_API_KEY) {
      return res.status(500).json({ error: "YT_API_KEY is not set (.env 확인)" });
    }

    // 1) 오늘 캐시가 있으면 그대로 반환
    const cached = readJson(CACHE_FILE, null);
    if (cached?.dayKey === todayKey() && Array.isArray(cached?.channels)) {
      return res.json({ ...cached, cached: true });
    }

    // 2) 없으면 새로 만들고 저장(=오늘 1번만)
    const fresh = await buildScheduleOncePerDay();
    writeJson(CACHE_FILE, fresh);

    return res.json({ ...fresh, cached: false });
  } catch (e) {
    return res.status(500).json({ error: String(e) });
  }
});

app.listen(PORT, () => console.log(`Server running on ${PORT}`));
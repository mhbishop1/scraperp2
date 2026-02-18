import { Actor } from 'apify';
import { ApifyClient } from 'apify-client';
import { createObjectCsvStringifier } from 'csv-writer';

await Actor.init();

const client = new ApifyClient({
    token: process.env.APIFY_TOKEN || 'YOUR_APIFY_API_TOKEN_HERE',
});

const input = await Actor.getInput() || {};
const maxVideos = input.maxVideos || 5; // Start small to save credits
const commentsPerVideo = 20;

console.log("🚀 Starting Two-Stage TikTok Research...");

try {
    // --- STAGE 1: FIND RELEVANT VIDEOS ---
    const searchInput = {
        "searchQueries": ["ChatGPT college argument", "AI cheating university", "roommate AI conflict"],
        "resultsPerPage": maxVideos,
        "shouldDownloadSubtitles": true
    };

    console.log("🔍 Stage 1: Finding videos...");
    const videoRun = await client.actor("clockworks/tiktok-scraper").call(searchInput);
    const { items: videos } = await client.dataset(videoRun.defaultDatasetId).listItems();

    const videoUrls = videos.map(v => v.webVideoUrl);
    console.log(`✅ Found ${videoUrls.length} videos. Moving to Stage 2...`);

    // --- STAGE 2: SCRAPE COMMENTS FOR THOSE VIDEOS ---
    console.log("💬 Stage 2: Scraping comments for discourse analysis...");
    const commentInput = {
        "postURLs": videoUrls,
        "commentsPerPost": commentsPerVideo,
        "maxRepliesPerComment": 2 // Captures small "sub-arguments"
    };

    const commentRun = await client.actor("clockworks/tiktok-comments-scraper").call(commentInput);
    const { items: comments } = await client.dataset(commentRun.defaultDatasetId).listItems();

    // --- DATA CLEANING & MERGING ---
    const finalResults = comments.map(c => ({
        video_url: c.videoUrl || "Unknown",
        comment_text: c.text,
        likes: c.diggCount,
        replies: c.repliesCount,
        author: c.authorUsername,
        timestamp: c.createTimeISO
    }));

    // --- STORAGE ---
    const csvStringifier = createObjectCsvStringifier({
        header: [
            { id: 'video_url', title: 'SOURCE_VIDEO' },
            { id: 'comment_text', title: 'COMMENT_CONTENT' },
            { id: 'likes', title: 'LIKES' },
            { id: 'replies', title: 'REPLY_COUNT' },
            { id: 'timestamp', title: 'DATE' }
        ]
    });

    const csv = csvStringifier.getHeaderString() + csvStringifier.stringifyRecords(finalResults);
    await Actor.setValue("TIKTOK_COMMENTS_RESEARCH.csv", csv, { contentType: "text/csv" });
    await Actor.pushData(finalResults);

    console.log(`🏁 Done! Gathered ${finalResults.length} comments across ${videoUrls.length} videos.`);

} catch (error) {
    console.error("❌ Research Scrape Failed:", error.message);
}

await Actor.exit();
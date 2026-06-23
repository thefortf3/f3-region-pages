import { kebabCase } from 'lodash';
import { NextRequest, NextResponse } from 'next/server';
import { revalidateTag } from 'next/cache';
import { eq } from 'drizzle-orm';

import { db } from '../../../../drizzle/db';
import { getPool as getWarehousePool } from '../../../../drizzle/f3-data-warehouse/db';
import { seedRuns, ingestRuns } from '../../../../drizzle/schema';
import { pruneRegions } from '../../../../scripts/prune-regions';
import { pruneWorkouts } from '../../../../scripts/prune-workouts';
import { seedRegions } from '../../../../scripts/seed-regions';
import { seedWorkouts } from '../../../../scripts/seed-workouts';
import { enrichRegions } from '../../../../scripts/enrich-regions';
import { getIngestComparison } from '../../../../scripts/ingest-analytics';
import { SITE_CONFIG } from '@/constants';

export const maxDuration = 300; // 5 minutes (requires Vercel Pro)

const INGEST_KEY = 'daily-ingest';
const FRESH_WINDOW_MS = 1000 * 60 * 60 * 20; // 20 hours (safe margin for daily runs)

const escapeSlack = (text: string) =>
  text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

const slackLink = (slug: string, text: string) =>
  slug
    ? `<${SITE_CONFIG.url}/${slug}|${escapeSlack(text)}>`
    : escapeSlack(text);

async function sendSlackNotification(message: string) {
  const token = process.env.SLACK_BOT_AUTH_TOKEN?.trim();
  const channel = process.env.SLACK_CHANNEL_ID?.trim();

  if (!token || !channel) {
    console.warn('Slack credentials not configured, skipping notification');
    return;
  }

  try {
    const response = await fetch('https://slack.com/api/chat.postMessage', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify({
        channel,
        text: message,
      }),
    });

    const result = await response.json();
    if (!result.ok) {
      console.error(
        `Slack notification failed: ${result.error} (channel: ${channel}, token: ${token ? 'set' : 'unset'})`
      );
    }
  } catch (error) {
    console.error('Failed to send Slack notification:', error);
  }
}

export async function POST(request: NextRequest) {
  // 1. Verify API key
  const authHeader = request.headers.get('authorization')?.trim();
  const cronSecret = process.env.CRON_SECRET?.trim();

  // Extract token from "Bearer <token>" format (case-insensitive)
  const token = authHeader?.replace(/^bearer\s+/i, '');

  if (!cronSecret || token !== cronSecret) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  try {
    // 2. Check if already run today (idempotent, unless ?force=true)
    const force = request.nextUrl.searchParams.get('force') === 'true';
    const [lastRun] = await db
      .select()
      .from(seedRuns)
      .where(eq(seedRuns.key, INGEST_KEY));

    if (!force && lastRun?.lastIngestedAt) {
      const elapsed = Date.now() - Date.parse(lastRun.lastIngestedAt);
      if (elapsed < FRESH_WINDOW_MS) {
        // Persist skipped run
        await db.insert(ingestRuns).values({
          startedAt: new Date().toISOString(),
          completedAt: new Date().toISOString(),
          status: 'skipped',
          durationSec: 0,
        });

        await sendSlackNotification(
          `:hourglass_flowing_sand: F3 Region Pages daily ingest skipped (already ran at ${lastRun.lastIngestedAt})`
        );
        return NextResponse.json({
          status: 'skipped',
          message: 'Already ingested today',
          lastIngestedAt: lastRun.lastIngestedAt,
        });
      }
    }

    // 3. Persist running state
    const startedAt = new Date().toISOString();
    const [runRow] = await db
      .insert(ingestRuns)
      .values({ startedAt, status: 'running' })
      .returning({ id: ingestRuns.id });

    // 4. Pre-flight: verify warehouse connectivity before running the pipeline
    const warehousePool = await getWarehousePool();
    let warehouseReachable = false;
    try {
      const client = await warehousePool.connect();
      try {
        await client.query('SELECT 1');
        warehouseReachable = true;
      } finally {
        client.release();
      }
    } catch {
      warehouseReachable = false;
    }
    if (!warehouseReachable) {
      const msg =
        'Warehouse database is unreachable after retry attempts — aborting ingest';
      console.error(msg);

      try {
        await db
          .update(ingestRuns)
          .set({
            completedAt: new Date().toISOString(),
            status: 'failure',
            durationSec: 0,
            errorMessage: msg,
          })
          .where(eq(ingestRuns.id, runRow.id));
      } catch (persistError) {
        console.error('Failed to persist pre-flight failure:', persistError);
      }

      await sendSlackNotification(
        `:x: F3 Region Pages daily ingest failed: ${escapeSlack(msg)}`
      );
      return NextResponse.json(
        { status: 'error', message: msg },
        { status: 503 }
      );
    }
    console.log('✅ Warehouse pre-flight check passed');

    // 5. Run ingest
    try {
      const startTime = Date.now();

      const pruneRegionsStats = await pruneRegions();
      const pruneWorkoutsStats = await pruneWorkouts();
      const seedRegionsStats = await seedRegions();
      const seedWorkoutsStats = await seedWorkouts();
      const enrichRegionsStats = await enrichRegions();

      revalidateTag('region-workouts');
      revalidateTag('regions');

      const durationSec = Math.round((Date.now() - startTime) / 1000);

      // 6. Record successful run in seedRuns
      const now = new Date().toISOString();
      await db
        .insert(seedRuns)
        .values({ key: INGEST_KEY, lastIngestedAt: now })
        .onConflictDoUpdate({
          target: seedRuns.key,
          set: { lastIngestedAt: now },
        });

      // 7. Build stats
      const stats = {
        durationSec,
        regionsPruned: pruneRegionsStats.removed,
        regionsPrunedNames: pruneRegionsStats.regionNames,
        workoutsPruned: pruneWorkoutsStats.removed,
        workoutsPrunedItems: pruneWorkoutsStats.workouts,
        regionsSeeded: seedRegionsStats.upserted,
        regionsSkippedFresh: seedRegionsStats.skippedFresh,
        regionsSeededNames: seedRegionsStats.regionNames,
        workoutsSeeded: seedWorkoutsStats.upserted,
        workoutsDeduplicated: seedWorkoutsStats.deduplicated,
        workoutsSkipped: seedWorkoutsStats.skipped,
        workoutBatches: seedWorkoutsStats.batches,
        workoutRegionBreakdown: seedWorkoutsStats.regionBreakdown,
        skipBreakdown: seedWorkoutsStats.skipBreakdown,
        regionsEnriched: enrichRegionsStats.enriched,
      };

      // 8. Get comparison analytics (before persisting success so current run isn't in the window)
      const comparison = await getIngestComparison({
        workoutsSeeded: stats.workoutsSeeded,
        workoutsSkipped: stats.workoutsSkipped,
        regionsSeeded: stats.regionsSeeded,
        durationSec: stats.durationSec,
      });

      // 9. Persist full stats to ingestRuns
      await db
        .update(ingestRuns)
        .set({
          completedAt: now,
          status: 'success',
          durationSec: stats.durationSec,
          regionsPruned: stats.regionsPruned,
          workoutsPruned: stats.workoutsPruned,
          regionsSeeded: stats.regionsSeeded,
          regionsSkippedFresh: stats.regionsSkippedFresh,
          workoutsSeeded: stats.workoutsSeeded,
          workoutsDeduplicated: stats.workoutsDeduplicated,
          workoutsSkipped: stats.workoutsSkipped,
          workoutBatches: stats.workoutBatches,
          workoutsSkippedFresh: stats.skipBreakdown.fresh,
          workoutsSkippedMissingType: stats.skipBreakdown.missingType,
          workoutsSkippedMissingAo: stats.skipBreakdown.missingAo,
          workoutsSkippedMissingRegion: stats.skipBreakdown.missingRegion,
          workoutsSkippedMissingLocation: stats.skipBreakdown.missingLocation,
          workoutsSkippedMissingGroup: stats.skipBreakdown.missingGroup,
          regionsEnriched: stats.regionsEnriched,
          workoutRegionBreakdown: JSON.stringify(stats.workoutRegionBreakdown),
        })
        .where(eq(ingestRuns.id, runRow.id));

      // 10. Build Slack message
      const fmt = (n: number) => n.toLocaleString('en-US');

      const capList = (
        items: { text: string; slug: string }[],
        max: number
      ) => {
        const formatted = items.map((item) => slackLink(item.slug, item.text));
        return formatted.length <= max
          ? formatted.join(', ')
          : `${formatted.slice(0, max).join(', ')} _and ${items.length - max} more_`;
      };

      const formatBreakdown = (
        breakdown: Record<string, number>,
        max: number
      ) => {
        const entries = Object.entries(breakdown).sort(([, a], [, b]) => b - a);
        const shown = entries.slice(0, max);
        const lines = shown.map(
          ([name, count]) =>
            `  • ${slackLink(kebabCase(name), name)}: ${fmt(count)}`
        );
        if (entries.length > max) {
          lines.push(`  • _and ${entries.length - max} more regions_`);
        }
        return lines.join('\n');
      };

      const regionsPrunedLine =
        stats.regionsPruned > 0
          ? `*Regions pruned (${fmt(stats.regionsPruned)}):* ${capList(
              stats.regionsPrunedNames.map((n) => ({
                text: n,
                slug: kebabCase(n),
              })),
              10
            )}`
          : `*Regions pruned:* 0`;

      const workoutsPrunedLine =
        stats.workoutsPruned > 0
          ? `*Workouts pruned (${fmt(stats.workoutsPruned)}):* ${capList(
              stats.workoutsPrunedItems.map((w) => ({
                text: w.name,
                slug: kebabCase(w.regionName),
              })),
              10
            )}`
          : `*Workouts pruned:* 0`;

      const regionsSeededLine =
        stats.regionsSeeded > 0
          ? `*Regions seeded (${fmt(stats.regionsSeeded)}):* ${capList(
              stats.regionsSeededNames.map((n) => ({
                text: n,
                slug: kebabCase(n),
              })),
              10
            )}` +
            (stats.regionsSkippedFresh > 0
              ? ` (${fmt(stats.regionsSkippedFresh)} skipped fresh)`
              : '')
          : `*Regions seeded:* 0` +
            (stats.regionsSkippedFresh > 0
              ? ` (${fmt(stats.regionsSkippedFresh)} skipped fresh)`
              : '');

      const workoutsSeededLine =
        `*Workouts seeded (${fmt(stats.workoutsSeeded)}):* ${fmt(stats.workoutsSeeded)} in ${fmt(stats.workoutBatches)} batch(es)` +
        (stats.workoutsDeduplicated > 0
          ? ` (${fmt(stats.workoutsDeduplicated)} deduplicated)`
          : '') +
        (stats.workoutsSkipped > 0
          ? ` (${fmt(stats.workoutsSkipped)} skipped)`
          : '');

      const breakdownSection =
        Object.keys(stats.workoutRegionBreakdown).length > 0
          ? '\n' + formatBreakdown(stats.workoutRegionBreakdown, 15)
          : '';

      // Skip breakdown line
      const sb = stats.skipBreakdown;
      const skipBreakdownLine =
        stats.workoutsSkipped > 0
          ? `*Skip breakdown:* fresh=${fmt(sb.fresh)}, missingType=${fmt(sb.missingType)}, missingAo=${fmt(sb.missingAo)}, missingRegion=${fmt(sb.missingRegion)}, missingLocation=${fmt(sb.missingLocation)}, missingGroup=${fmt(sb.missingGroup)}`
          : '';

      // Comparison lines
      const comparisonLines: string[] = [];
      if (comparison.deltas) {
        const d = comparison.deltas;
        const sign = d.workoutsSeeded >= 0 ? '+' : '';
        const pctStr =
          d.workoutsSeededPct !== 0
            ? ` (${sign}${d.workoutsSeededPct.toFixed(0)}%)`
            : '';
        comparisonLines.push(
          `*vs last run:* workouts ${sign}${fmt(d.workoutsSeeded)}${pctStr}, skip rate ${(comparison.skipRate * 100).toFixed(1)}%`
        );
      }
      if (comparison.rolling.sampleSize >= 2) {
        comparisonLines.push(
          `*${comparison.rolling.sampleSize}-run avg:* ${fmt(Math.round(comparison.rolling.mean))} seeded (stddev ${fmt(Math.round(comparison.rolling.stddev))})`
        );
      }
      if (comparison.anomaly.flagged) {
        comparisonLines.push(
          `:warning: *ANOMALY:* ${comparison.anomaly.message}`
        );
      }

      const comparisonSection =
        comparisonLines.length > 0 ? '\n' + comparisonLines.join('\n') : '';

      await sendSlackNotification(
        `:white_check_mark: F3 Region Pages daily ingest completed\n\n` +
          `*Duration:* ${durationSec}s\n` +
          `${regionsPrunedLine}\n` +
          `${workoutsPrunedLine}\n` +
          `${regionsSeededLine}\n` +
          `${workoutsSeededLine}${breakdownSection}\n` +
          (skipBreakdownLine ? `${skipBreakdownLine}\n` : '') +
          `*Regions enriched:* ${fmt(stats.regionsEnriched)}` +
          comparisonSection
      );

      const {
        regionsPrunedNames: _rpn, // eslint-disable-line @typescript-eslint/no-unused-vars
        workoutsPrunedItems: _wpi, // eslint-disable-line @typescript-eslint/no-unused-vars
        regionsSeededNames: _rsn, // eslint-disable-line @typescript-eslint/no-unused-vars
        workoutRegionBreakdown: _wrb, // eslint-disable-line @typescript-eslint/no-unused-vars
        skipBreakdown: _sb, // eslint-disable-line @typescript-eslint/no-unused-vars
        ...summaryStats
      } = stats;

      return NextResponse.json({
        status: 'success',
        message: 'Ingest completed',
        completedAt: now,
        stats: summaryStats,
        comparison,
      });
    } catch (error) {
      console.error('Ingest failed:', error);

      // Persist failure (best-effort — don't let this swallow the original error)
      try {
        await db
          .update(ingestRuns)
          .set({
            completedAt: new Date().toISOString(),
            status: 'failure',
            durationSec: Math.round(
              (Date.now() - Date.parse(startedAt)) / 1000
            ),
            errorMessage: String(error),
          })
          .where(eq(ingestRuns.id, runRow.id));
      } catch (persistError) {
        console.error('Failed to persist ingest failure:', persistError);
      }

      // Send failure notification
      await sendSlackNotification(
        `:x: F3 Region Pages daily ingest failed: ${escapeSlack(String(error))}`
      );

      return NextResponse.json(
        { status: 'error', message: String(error) },
        { status: 500 }
      );
    }
  } catch (outerError) {
    // Catches errors in the preamble (idempotency check, ingestRuns insert)
    // that would otherwise crash without any Slack notification
    console.error('Ingest handler crashed:', outerError);

    await sendSlackNotification(
      `:x: F3 Region Pages ingest handler crashed (pre-ingest): ${escapeSlack(String(outerError))}`
    );

    return NextResponse.json(
      { status: 'error', message: String(outerError) },
      { status: 500 }
    );
  }
}

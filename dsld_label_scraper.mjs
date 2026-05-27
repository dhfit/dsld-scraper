/**
 * NIH DSLD full label scraper — sharded GitHub Actions edition
 * Env vars: SHARD_INDEX (0-based), TOTAL_SHARDS
 * Reads IDs from dsld_ids.txt, processes only this shard's slice.
 * Outputs: dsld_products_shard_N.csv, dsld_ingredients_shard_N.csv
 * Checkpoint: dsld_checkpoint_shard_N.json
 */

import fs from 'fs';

const BASE         = 'https://api.ods.od.nih.gov/dsld/v9/label';
const IDS_FILE     = './dsld_ids.txt';
const SHARD        = parseInt(process.env.SHARD_INDEX  ?? '0', 10);
const TOTAL_SHARDS = parseInt(process.env.TOTAL_SHARDS ?? '1', 10);
const OUT_PROD     = `./dsld_products_shard_${SHARD}.csv`;
const OUT_ING      = `./dsld_ingredients_shard_${SHARD}.csv`;
const CHECKPOINT   = `./dsld_checkpoint_shard_${SHARD}.json`;
const DELAY_MS     = 1500;
const CHECKPOINT_EVERY = 250;

// Stop this many ms before the job timeout so the process exits cleanly,
// letting the post-job cache-save step always run. Job timeout = 290 min.
const GRACEFUL_STOP_MS = 265 * 60 * 1000;

// ─── CSV helpers ──────────────────────────────────────────────────────────────

function cell(val) {
  const s = String(val ?? '');
  return (s.includes(',') || s.includes('"') || s.includes('\n'))
    ? '"' + s.replace(/"/g, '""') + '"' : s;
}
function csvRow(headers, obj) {
  return headers.map(h => cell(obj[h] ?? '')).join(',') + '\n';
}

const PROD_HEADERS = [
  'id','product_name','brand','nhanes_id','upc_sku','off_market','entry_date',
  'product_type','physical_state','serving_qty','serving_unit',
  'servings_per_container','net_quantity','target_groups',
  'user_groups','claims','statements','labeled_ingredient_count','other_ingredient_count',
];
const ING_HEADERS = [
  'product_id','ingredient_id','parent_ingredient_id','name','ingredient_group',
  'category','order','quantity','unit','operator','percent_dv','dv_target_group',
  'notes','forms','is_active',
];

// ─── label parsing ────────────────────────────────────────────────────────────

function parseProduct(d) {
  const serving    = (d.servingSizes ?? [])[0] ?? {};
  const netContent = (d.netContents ?? []).sort((a,b)=>(a.order??0)-(b.order??0))[0];
  return {
    id: String(d.id ?? ''),
    product_name: d.fullName ?? '',
    brand: d.brandName ?? '',
    nhanes_id: d.nhanesId ?? '',
    upc_sku: d.upcSku ?? '',
    off_market: d.offMarket ? '1' : '0',
    entry_date: d.entryDate ?? '',
    product_type: d.productType?.langualCodeDescription ?? '',
    physical_state: d.physicalState?.langualCodeDescription ?? '',
    serving_qty: serving.minQuantity != null
      ? (serving.minQuantity === serving.maxQuantity
          ? String(serving.minQuantity)
          : `${serving.minQuantity}-${serving.maxQuantity}`)
      : '',
    serving_unit: serving.unit ?? '',
    servings_per_container: d.servingsPerContainer != null ? String(d.servingsPerContainer) : '',
    net_quantity: netContent?.display ?? '',
    target_groups: (d.targetGroups ?? []).join('; '),
    user_groups: (d.userGroups ?? []).map(g => g.dailyValueTargetGroupName ?? '').filter(Boolean).join('; '),
    claims: (d.claims ?? []).map(c => c.langualCodeDescription).filter(Boolean).join('; '),
    statements: (d.statements ?? []).map(s => s.notes).filter(Boolean).join('; '),
    labeled_ingredient_count: String((d.ingredientRows ?? []).length),
    other_ingredient_count: String((d.otheringredients?.ingredients ?? []).length),
  };
}

function parseIngRow(productId, row, parentId = '') {
  const q = (row.quantity ?? [])[0] ?? {};
  const dv = (q.dailyValueTargetGroup ?? [])[0] ?? {};
  return {
    product_id: productId,
    ingredient_id: String(row.ingredientId ?? ''),
    parent_ingredient_id: parentId,
    name: row.name ?? '',
    ingredient_group: row.ingredientGroup ?? '',
    category: row.category ?? '',
    order: String(row.order ?? ''),
    quantity: q.quantity != null ? String(q.quantity) : '',
    unit: q.unit ?? '',
    operator: q.operator ?? '',
    percent_dv: dv.percent != null ? String(dv.percent) : '',
    dv_target_group: dv.name ?? '',
    notes: row.notes ?? '',
    forms: (row.forms ?? []).map(f => f.name).filter(Boolean).join('; '),
    is_active: '1',
  };
}

function parseOtherIng(productId, ing, order) {
  return {
    product_id: productId, ingredient_id: String(ing.ingredientId ?? ''),
    parent_ingredient_id: '', name: ing.name ?? '',
    ingredient_group: ing.ingredientGroup ?? '', category: ing.category ?? '',
    order: String(order), quantity: '', unit: '', operator: '',
    percent_dv: '', dv_target_group: '', notes: '',
    forms: (ing.forms ?? []).map(f => f.name).filter(Boolean).join('; '),
    is_active: '0',
  };
}

function extractIngredients(productId, data) {
  const rows = [];
  for (const ir of data.ingredientRows ?? []) {
    rows.push(parseIngRow(productId, ir));
    for (const nested of ir.nestedRows ?? [])
      rows.push(parseIngRow(productId, nested, String(ir.ingredientId ?? '')));
  }
  let o = 1;
  for (const oi of data.otheringredients?.ingredients ?? [])
    rows.push(parseOtherIng(productId, oi, o++));
  return rows;
}

// ─── fetch ────────────────────────────────────────────────────────────────────

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function fetchLabel(id, retries = 8) {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const r = await fetch(`${BASE}/${id}`, { signal: AbortSignal.timeout(25000) });
      if (r.status === 429) {
        const retryAfter = parseInt(r.headers.get('Retry-After') ?? '120', 10);
        console.log(`\n  [shard ${SHARD}] 429 — sleeping ${retryAfter + 10}s`);
        await sleep((retryAfter + 10) * 1000);
        continue;
      }
      if (r.status === 404) return null;
      if (!r.ok) { await sleep(3000); continue; }
      return r.json();
    } catch { await sleep(5000 * (attempt + 1)); }
  }
  return null;
}

// ─── main ─────────────────────────────────────────────────────────────────────

async function main() {
  const JOB_START = Date.now();
  console.log(`=== DSLD Scraper — Shard ${SHARD}/${TOTAL_SHARDS} ===`);

  // Load all IDs and slice this shard's portion
  const allIds = fs.readFileSync(IDS_FILE, 'utf8').split('\n').map(s => s.trim()).filter(Boolean);
  const chunkSize = Math.ceil(allIds.length / TOTAL_SHARDS);
  const shardIds  = allIds.slice(SHARD * chunkSize, (SHARD + 1) * chunkSize);
  console.log(`Shard IDs: ${shardIds.length} (indices ${SHARD * chunkSize}–${Math.min((SHARD+1)*chunkSize, allIds.length)-1})`);

  // Load checkpoint
  let done = new Set();
  if (fs.existsSync(CHECKPOINT)) {
    done = new Set(JSON.parse(fs.readFileSync(CHECKPOINT, 'utf8')).done ?? []);
    console.log(`Resuming: ${done.size} already done.`);
  }

  const pending = shardIds.filter(id => !done.has(id));
  console.log(`Pending: ${pending.length}\n`);
  if (pending.length === 0) { console.log('Shard complete!'); return; }

  // Open output files (append if CSV already exists from a prior run)
  const prodExists = fs.existsSync(OUT_PROD) && fs.statSync(OUT_PROD).size > 0;
  const ingExists  = fs.existsSync(OUT_ING)  && fs.statSync(OUT_ING).size  > 0;
  const prodWriter = fs.createWriteStream(OUT_PROD, { flags: prodExists ? 'a' : 'w', encoding: 'utf8' });
  const ingWriter  = fs.createWriteStream(OUT_ING,  { flags: ingExists  ? 'a' : 'w', encoding: 'utf8' });
  if (!prodExists) prodWriter.write(PROD_HEADERS.join(',') + '\n');
  if (!ingExists)  ingWriter.write(ING_HEADERS.join(',') + '\n');

  let processed = 0, errors = 0, sinceCheckpoint = 0;
  const startTime = Date.now();
  let timedOut = false;

  for (const id of pending) {
    // Graceful early exit: stop 25 min before job timeout so post-job cache
    // save always runs. Without this the job is force-killed mid-run and the
    // checkpoint is lost, wasting the entire run's work.
    if (Date.now() - JOB_START >= GRACEFUL_STOP_MS) {
      console.log(`\n\nApproaching time limit (265 min) — saving checkpoint and stopping gracefully.`);
      timedOut = true;
      break;
    }

    const data = await fetchLabel(id);
    await sleep(DELAY_MS);

    if (!data) errors++;
    else {
      const prod = parseProduct(data);
      const ings = extractIngredients(String(data.id ?? id), data);
      prodWriter.write(csvRow(PROD_HEADERS, prod));
      for (const ing of ings) ingWriter.write(csvRow(ING_HEADERS, ing));
    }

    done.add(id);
    processed++;
    sinceCheckpoint++;

    if (sinceCheckpoint >= CHECKPOINT_EVERY) {
      fs.writeFileSync(CHECKPOINT, JSON.stringify({ done: [...done] }));
      sinceCheckpoint = 0;
    }

    const elapsed = (Date.now() - startTime) / 1000;
    const rate = processed / elapsed;
    const remaining = (pending.length - processed) / rate;
    const eta = new Date(Date.now() + remaining * 1000).toLocaleTimeString();
    process.stdout.write(`\r[S${SHARD}] ${processed}/${pending.length} (${((processed/pending.length)*100).toFixed(1)}%) | ${rate.toFixed(2)} req/s | ETA ${eta} | errors: ${errors}  `);
  }

  fs.writeFileSync(CHECKPOINT, JSON.stringify({ done: [...done] }));
  prodWriter.end(); ingWriter.end();

  if (timedOut) {
    console.log(`\n=== Shard ${SHARD} PAUSED (time limit) === processed this run: ${processed}, total done: ${done.size}/${shardIds.length}, errors: ${errors}`);
  } else {
    console.log(`\n\n=== Shard ${SHARD} DONE === processed: ${processed}, errors: ${errors}`);
  }
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });

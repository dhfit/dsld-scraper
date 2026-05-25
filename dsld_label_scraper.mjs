/**
 * NIH DSLD full label scraper — GitHub Actions edition
 * Reads IDs from dsld_ids.txt, fetches /v9/label/{id} for each,
 * writes two normalized CSVs (appends if resuming from checkpoint).
 *
 * Resumable via dsld_label_checkpoint.json (persisted in GH Actions cache).
 * Concurrency: 1 worker at 1 req/s to stay under NIH rate limits.
 */

import fs from 'fs';

const BASE = 'https://api.ods.od.nih.gov/dsld/v9/label';
const IDS_FILE    = './dsld_ids.txt';
const OUT_PROD    = './dsld_products_full.csv';
const OUT_ING     = './dsld_ingredients_full.csv';
const CHECKPOINT  = './dsld_label_checkpoint.json';
const DELAY_MS    = 1100;
const CHECKPOINT_EVERY = 500;

// ─── CSV helpers ──────────────────────────────────────────────────────────────

function cell(val) {
  const s = String(val ?? '');
  return (s.includes(',') || s.includes('"') || s.includes('\n'))
    ? '"' + s.replace(/"/g, '""') + '"' : s;
}
function row(headers, obj) {
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
  const serving = (d.servingSizes ?? [])[0] ?? {};
  const netContent = (d.netContents ?? []).sort((a,b) => (a.order??0)-(b.order??0))[0];
  const claims = (d.claims ?? []).map(c => c.langualCodeDescription).filter(Boolean).join('; ');
  const statements = (d.statements ?? []).map(s => s.notes).filter(Boolean).join('; ');
  const targetGroups = (d.targetGroups ?? []).join('; ');
  const userGroups = (d.userGroups ?? []).map(g => g.dailyValueTargetGroupName ?? '').filter(Boolean).join('; ');
  return {
    id: String(d.id ?? ''), product_name: d.fullName ?? '', brand: d.brandName ?? '',
    nhanes_id: d.nhanesId ?? '', upc_sku: d.upcSku ?? '',
    off_market: d.offMarket ? '1' : '0', entry_date: d.entryDate ?? '',
    product_type: d.productType?.langualCodeDescription ?? '',
    physical_state: d.physicalState?.langualCodeDescription ?? '',
    serving_qty: serving.minQuantity != null
      ? (serving.minQuantity === serving.maxQuantity ? String(serving.minQuantity) : `${serving.minQuantity}-${serving.maxQuantity}`) : '',
    serving_unit: serving.unit ?? '',
    servings_per_container: d.servingsPerContainer != null ? String(d.servingsPerContainer) : '',
    net_quantity: netContent?.display ?? '',
    target_groups: targetGroups, user_groups: userGroups, claims, statements,
    labeled_ingredient_count: String((d.ingredientRows ?? []).length),
    other_ingredient_count: String((d.otheringredients?.ingredients ?? []).length),
  };
}

function parseIngredientRow(productId, row, parentId = '') {
  const q = (row.quantity ?? [])[0] ?? {};
  const dvGroup = (q.dailyValueTargetGroup ?? [])[0] ?? {};
  return {
    product_id: productId, ingredient_id: String(row.ingredientId ?? ''),
    parent_ingredient_id: parentId, name: row.name ?? '',
    ingredient_group: row.ingredientGroup ?? '', category: row.category ?? '',
    order: String(row.order ?? ''),
    quantity: q.quantity != null ? String(q.quantity) : '', unit: q.unit ?? '',
    operator: q.operator ?? '',
    percent_dv: dvGroup.percent != null ? String(dvGroup.percent) : '',
    dv_target_group: dvGroup.name ?? '', notes: row.notes ?? '',
    forms: (row.forms ?? []).map(f => f.name).filter(Boolean).join('; '),
    is_active: '1',
  };
}

function parseOtherIngredient(productId, ing, order) {
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
    rows.push(parseIngredientRow(productId, ir));
    for (const nested of ir.nestedRows ?? []) {
      rows.push(parseIngredientRow(productId, nested, String(ir.ingredientId ?? '')));
    }
  }
  let otherOrder = 1;
  for (const oi of data.otheringredients?.ingredients ?? []) {
    rows.push(parseOtherIngredient(productId, oi, otherOrder++));
  }
  return rows;
}

// ─── fetch with retry + Retry-After ──────────────────────────────────────────

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function fetchLabel(id, retries = 6) {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const r = await fetch(`${BASE}/${id}`, { signal: AbortSignal.timeout(25000) });
      if (r.status === 429) {
        const retryAfter = parseInt(r.headers.get('Retry-After') ?? '120', 10);
        const wait = (retryAfter + 10) * 1000;
        console.log(`\n  429 on ${id} — sleeping ${Math.round(wait/1000)}s`);
        await sleep(wait);
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
  console.log('=== NIH DSLD Label Scraper ===');

  // Load checkpoint
  let done = new Set();
  if (fs.existsSync(CHECKPOINT)) {
    const raw = JSON.parse(fs.readFileSync(CHECKPOINT, 'utf8'));
    done = new Set(raw.done ?? []);
    console.log(`Resuming: ${done.size} already done.`);
  }
  const isResume = done.size > 0;

  // Load IDs
  const allIds = fs.readFileSync(IDS_FILE, 'utf8').split('\n').map(s => s.trim()).filter(Boolean);
  const pending = allIds.filter(id => !done.has(id));
  console.log(`Total: ${allIds.length} | Pending: ${pending.length}\n`);

  if (pending.length === 0) {
    console.log('All done!');
    return;
  }

  // Open output files — base mode on file existence, not checkpoint
  const prodExists = fs.existsSync(OUT_PROD) && fs.statSync(OUT_PROD).size > 0;
  const ingExists  = fs.existsSync(OUT_ING)  && fs.statSync(OUT_ING).size  > 0;
  const prodWriter = fs.createWriteStream(OUT_PROD, { flags: prodExists ? 'a' : 'w', encoding: 'utf8' });
  const ingWriter  = fs.createWriteStream(OUT_ING,  { flags: ingExists  ? 'a' : 'w', encoding: 'utf8' });
  if (!prodExists) prodWriter.write(PROD_HEADERS.join(',') + '\n');
  if (!ingExists)  ingWriter.write(ING_HEADERS.join(',') + '\n');

  let processed = 0, errors = 0, sinceCheckpoint = 0;
  const startTime = Date.now();

  for (const id of pending) {
    const data = await fetchLabel(id);
    await sleep(DELAY_MS);

    if (!data) { errors++; }
    else {
      const prod = parseProduct(data);
      const ings = extractIngredients(String(data.id ?? id), data);
      prodWriter.write(row(PROD_HEADERS, prod));
      for (const ing of ings) ingWriter.write(row(ING_HEADERS, ing));
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
    process.stdout.write(
      `\r${processed}/${pending.length} (${((processed/pending.length)*100).toFixed(1)}%) | ` +
      `${rate.toFixed(2)} req/s | ETA ${eta} | errors: ${errors}  `
    );
  }

  // Final save
  fs.writeFileSync(CHECKPOINT, JSON.stringify({ done: [...done] }));
  prodWriter.end(); ingWriter.end();

  console.log(`\n\n=== DONE ===`);
  console.log(`Processed: ${processed} | Errors: ${errors}`);
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });

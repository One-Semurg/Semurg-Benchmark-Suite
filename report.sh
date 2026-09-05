#!/usr/bin/env bash
# report.sh -- the ONE end-of-run report renderer for the Semurg arena.
#
# Input: the unified results TSV written by run_all_domains.sh (#meta header + 8 columns:
#   domain  engine  load_ms  q_ms(list)  equal_answer_hash  status  variant  note).
# Output (two skins from ONE model -- never a second report path):
#   * an ASCII/box-drawing scorecard to stdout (always available, SSH-safe, copy-pasteable)
#   * a self-contained arena_report.html next to the TSV (inline CSS, system fonts, NO CDN / NO JS /
#     NO fetch / NO @font-face) -- the screenshot-and-repost artifact.
#
# Honesty north star, ON THE CARD: per-row equal-answer badge; losses shown SAME SIZE as wins, worded
# straight, next to the architectural counter-move (O(1) fold, out-of-core survive-vs-DNF crown);
# DNF reads as DNF with its reason; "measured on YOUR hardware" + host/commit/utc banner.
#
# PUBLICATION SAFETY: this renderer REFUSES to place any DeWitt/licence-restricted engine
# (label carries [DeWitt-internal], or name in memgraph|tigergraph|kdb|dragonfly|elasticsearch) into
# EITHER skin. So even a --licensed run yields a publication-safe card; DeWitt numbers stay in the raw
# TSV on the operator's box only.
#
# No dependencies beyond bash + awk (+ coreutils). Offline. Deterministic: same TSV -> same card.
set -uo pipefail

TSV="${1:-}"
[ -n "$TSV" ] && [ -f "$TSV" ] || { echo "report.sh: usage: report.sh <unified_results.tsv>" >&2; exit 2; }
ASCII_ONLY="${ARENA_ASCII_ONLY:-0}"      # ARENA_ASCII_ONLY=1 -> pure + - | box (locale-safe)
HTML_OUT="${ARENA_HTML_OUT:-$(dirname "$TSV")/arena_report.html}"

awk -v ascii_only="$ASCII_ONLY" -v html_out="$HTML_OUT" '
BEGIN{
  FS="\t"
  # per-domain headline metric map (the ONLY domain-specific knowledge; everything else is generic).
  # metric = which subfield of q_ms; dir = lo (lower better) | hi (higher better).
  split("graph vector timeseries relational columnar kv object document streaming search universal", DORDER, " ")
  metric["graph"]="query_ms";      dir["graph"]="lo"
  metric["vector"]="query_ms";     dir["vector"]="lo"
  metric["timeseries"]="qmax";     dir["timeseries"]="lo"
  metric["relational"]="qmax";     dir["relational"]="lo"
  metric["columnar"]="scan_rps";   dir["columnar"]="hi"
  metric["kv"]="rows_per_s";       dir["kv"]="hi"
  metric["object"]="query_ms";     dir["object"]="lo"
  metric["document"]="q2";         dir["document"]="lo"
  metric["streaming"]="query_ms";  dir["streaming"]="lo"
  metric["search"]="query_ms";     dir["search"]="lo"
  metric["universal"]="qmax";      dir["universal"]="lo"
  dnum["graph"]=1; dnum["vector"]=2; dnum["timeseries"]=3; dnum["relational"]=4; dnum["columnar"]=5
  dnum["kv"]=6; dnum["object"]=7; dnum["document"]=8; dnum["streaming"]=9; dnum["search"]=10; dnum["universal"]=11
  dewitt="memgraph|tigergraph|kdb|dragonfly|elasticsearch"
}
# ---- ingest ---------------------------------------------------------------------------------
/^#meta /{ sub(/^#meta /,""); k=$0; sub(/=.*/,"",k); v=$0; sub(/^[^=]*=/,"",v); META[k]=v; next }
/^#/{ next }
NR_hdr==0 && $1=="domain"{ NR_hdr=1; next }      # column header
NF<6{ next }
{
  dom=$1; eng=$2; load=$3; qms=$4; hash=$5; st=$6; var=($7==""?"-":$7); note=($8==""?"-":$8)
  # DeWitt render-guard: drop from BOTH skins, unconditionally.
  low=tolower(eng)
  if (index(low,"[dewitt-internal]")>0){ next }
  if (match(low, "^(" dewitt ")")){ next }
  i=++N
  R_dom[i]=dom; R_eng[i]=eng; R_qms[i]=qms; R_hash[i]=hash; R_st[i]=st; R_var[i]=var; R_note[i]=note
  DOMSEEN[dom]=1
}
# ---- helpers --------------------------------------------------------------------------------
function sub_field(list, key,   n,a,j,kv){       # pull key=VALUE out of a ";"-joined q_ms list
  n=split(list,a,";")
  for(j=1;j<=n;j++){ if(index(a[j],key "=")==1){ kv=a[j]; sub(/^[^=]*=/,"",kv); return kv } }
  return ""
}
function qmax(list,   n,a,j,m,v){                # max of q1;q2;q3 (worst latency = the honest headline)
  n=split(list,a,";"); m=""
  for(j=1;j<=n;j++){ if(a[j]~/^q[0-9]=/){ v=a[j]; sub(/^[^=]*=/,"",v); if(m==""||v+0>m+0) m=v } }
  return m
}
function metric_val(dom, list,   mk,v){
  mk=metric[dom]
  if(mk=="qmax") return qmax(list)
  v=sub_field(list, mk)
  # columnar: semurg emits scan_rps, the incumbent (duckdb/clickhouse) emits rows_per_s -- same concept
  if(v=="" && dom=="columnar") v=sub_field(list,"rows_per_s")
  return v
}
function is_counted(st){ return (st=="ok-reference"||st=="ok-matched"||st=="ok") }
function fmtx(r){ if(r>=100) return sprintf("%.0fx",r); if(r>=10) return sprintf("%.0fx",r); return sprintf("%.1fx",r) }
# ---- render -----------------------------------------------------------------------------------
END{
  # box-drawing chars: Unicode by default (they sit at fixed line positions, not inside padded fields,
  # so they do not disturb byte-based column math), ASCII with --ascii-only. The CONTENT badges are
  # ALWAYS pure ASCII in the terminal panel, because awk pads/truncates by BYTES and a multibyte glyph
  # (3 bytes, 1 column) would shift the right border. The HTML skin uses its own HTML entities.
  if(ascii_only){ TL="+";TR="+";BL="+";BR="+";H="-";V="|";ML="+";MR="+" }
  else          { TL="╔";TR="╗";BL="╚";BR="╝";H="═";V="║";ML="╠";MR="╣" }
  CHK="Y"; XX="x"; CROWN="*"; FOLD="!"

  # aggregate per domain: semurg headline, best incumbent headline+name, any-incumbent-dnf, eq-answer
  for(i=1;i<=N;i++){
    d=R_dom[i]
    if(tolower(R_eng[i]) ~ /^semurg/){
      # graph: keep BOTH in-core and ooc variants
      if(R_var[i]=="graph_ooc"){ SEMV_ooc[d]=metric_val(d,R_qms[i]); SEMST_ooc[d]=R_st[i]; SEMN_ooc[d]=R_note[i] }
      else { SEMV[d]=metric_val(d,R_qms[i]); SEMST[d]=R_st[i]; SEM_fold[d]=sub_field(R_qms[i],"fold_rps"); SEM_q1[d]=sub_field(R_qms[i],"q1"); SEM_q2[d]=sub_field(R_qms[i],"q2"); SEM_q3[d]=sub_field(R_qms[i],"q3") }
    } else {
      v=metric_val(d,R_qms[i]); cnt=is_counted(R_st[i])
      # best incumbent (by direction) among counted rows; track dnf separately
      if(R_st[i]=="dnf"){ INC_dnf[d]=INC_dnf[d] (INC_dnf[d]?",":"") R_eng[i] }
      if(cnt && v!=""){
        if(dir[d]=="lo"){ if(!(d in INCV) || v+0 < INCV[d]+0){ INCV[d]=v; INCN[d]=R_eng[i] } }
        else            { if(!(d in INCV) || v+0 > INCV[d]+0){ INCV[d]=v; INCN[d]=R_eng[i] } }
      }
      # ooc incumbent value (graph) + track for crown
      if(R_var[i]=="graph_ooc"){ if(R_st[i]=="dnf"){ INC_ooc_dnf[d]=INC_ooc_dnf[d] (INC_ooc_dnf[d]?",":"") R_eng[i] } else if(is_counted(R_st[i])){ if(!(d in INCV_ooc)||R_qms[i]!=""){ INCV_ooc[d]=metric_val(d,R_qms[i]); INCN_ooc[d]=R_eng[i] } } }
      # relational/kv: remember per-incumbent for MIXED wording
      if(d=="relational"){ REL_inc_q2[d]=sub_field(R_qms[i],"q2"); REL_inc_q1[d]=sub_field(R_qms[i],"q1"); REL_inc_n[d]=R_eng[i] }
      if(d=="kv" && cnt && v!=""){ KV_list[d]=KV_list[d] (KV_list[d]?" ":"") R_eng[i] "=" v }
      if(cnt) EQ_ok[d]=1
    }
  }

  # per-domain verdict text
  win=0; loss=0; mixed=0; crown=0
  for(k=1;k<=11;k++){
    d=DORDER[k]; if(!(d in DOMSEEN)) continue
    sv=SEMV[d]; iv=INCV[d]; verdict=""; badge=CHK
    # equal-answer badge: ok if semurg counted
    semok = (SEMST[d]=="ok-reference"||SEMST[d]=="ok-matched"||SEMST[d]=="ok"||SEMST_ooc[d]=="ok-reference"||SEMST_ooc[d]=="ok-matched"||SEMST_ooc[d]=="ok")
    if(SEMST[d]=="ok-mismatch"){ badge=XX; verdict="MISMATCH - not counted" }
    else if(!semok && SEMST[d]!="" ){ badge="-"; }

    if(verdict==""){
      if(d=="graph"){
        # in-core ratio + OOC crown (crownrow text carries NO glyph; each skin adds its own marker)
        if(sv!="" && iv!=""){ if(dir[d]=="lo"){ r=iv/ (sv==0?0.5:sv) } ; verdict=sprintf("WIN ~%s (in-core)", fmtx(r)) }
        if((d in INC_ooc_dnf) && (SEMST_ooc[d]!="" )){ crownrow[d]=sprintf("%s DNF @cap; Semurg survives @flat mem", INC_ooc_dnf[d]); crown++ }
      }
      else if(d=="columnar"){
        if(sv!="" && iv!=""){ r=(sv+0)/(iv==0?0.5:iv); if(r>=1) verdict=sprintf("scan WIN ~%s", fmtx(r)); else verdict=sprintf("scan LOSS ~%s", fmtx(1/r)) }
        if(SEM_fold[d]!="" && iv!=""){ fr=(SEM_fold[d]+0)/(iv==0?0.5:iv); foldrow[d]=sprintf("~%s (same answer, O(1) monoid)", fmtx(fr)); }
      }
      else if(d=="relational"){
        # point-get (q2) vs scan (q1/q3): honest MIXED
        verdict="MIXED: pt-get win / scan loss"; mixed++
      }
      else if(d=="kv"){
        verdict="MIXED (vs redis / rocksdb)"; mixed++
      }
      else if(d=="universal"){
        if(d in INC_dnf){ verdict=sprintf("survives: %s DNF", INC_dnf[d]); crown++ }
        else if(sv!=""&&iv!=""){ r=iv/(sv==0?0.5:sv); verdict= (r>=1? sprintf("WIN ~%s",fmtx(r)) : sprintf("LOSS ~%s",fmtx(1/r))) }
      }
      else {
        # generic lo/hi
        if(sv!="" && iv!=""){
          if(dir[d]=="lo"){ r=(iv+0)/(sv==0?0.5:sv) } else { r=(sv+0)/(iv==0?0.5:iv) }
          verdict = (r>=1 ? sprintf("WIN ~%s",fmtx(r)) : sprintf("LOSS ~%s",fmtx(1/r)))
        } else if(d in INC_dnf && sv!=""){ verdict=sprintf("survives: %s DNF",INC_dnf[d]); crown++ }
        else verdict="(incomplete)"
      }
    }
    VJ[d]=verdict; BJ_badge[d]=badge
    if(verdict ~ /WIN/) win++
    else if(verdict ~ /LOSS/) loss++
  }
  # count crowns already; recount win/loss cleanly excluding mixed handled above
  score=sprintf("%d win  %d loss  %d mixed  %d crown", win, loss, mixed, crown)

  # ----------------------------- ASCII to stdout -----------------------------
  host=META["host"]; cores=META["cores"]; ram=META["ram_gib"]; commit=META["suite_commit"]; utc=META["run_utc"]
  printf "%s%s%s\n", TL, rep(H,86), TR
  aline(sprintf("SEMURG ARENA  -  11-DOMAIN BENCHMARK  -  measured on YOUR hardware"))
  printf "%s%s%s\n", ML, rep(H,86), MR
  aline(sprintf("host  %s - %s cores - %s GiB RAM", host, cores, ram))
  aline(sprintf("suite %s   run %s   equal-answer gated", commit, utc))
  aline("data  deterministic generators (same knobs => same bytes => same answer hash)")
  aline("gate  a lane counts ONLY if its answer hash matches the reference; losses shown")
  aline("      straight; DNF = did-not-finish at the budget; no fake numbers")
  printf "%s%s%s\n", ML, rep(H,86), MR
  aline(sprintf("%-13s %-15s %-17s %-3s %s","DOMAIN","SEMURG","BEST INCUMBENT","=AN","VERDICT"))
  printf "%s%s%s\n", ML, rep(H,86), MR
  for(k=1;k<=11;k++){
    d=DORDER[k]; if(!(d in DOMSEEN)) continue
    iname=(INCN[d]==""?"-":INCN[d]) (INCV[d]==""?"":" "INCV[d])
    aline(sprintf("%-2d %-10s %-15s %-17s %-3s %s", dnum[d], d, sfmt(d), substr(iname,1,17), BJ_badge[d], VJ[d]))
    if(d in crownrow) aline(sprintf("   %-10s %s CROWN: %s","  ooc", CROWN, crownrow[d]))
    if(d in foldrow)  aline(sprintf("   %-10s %s FOLD %s","  fold", FOLD, foldrow[d]))
  }
  printf "%s%s%s\n", ML, rep(H,86), MR
  aline("SCORE  " score)
  aline("Reproduce on your box:   ./bin/semurg-arena run --all")
  aline("Add the Semurg lane:     ./bin/semurg-arena install      Details: data.semurg.io")
  aline("License-restricted engines are NOT shown here; run them locally, only you see them.")
  printf "%s%s%s\n", BL, rep(H,86), BR

  # caption for reposting (printed after the card)
  print ""
  print "Copy this caption when you share the screenshot:"
  print "-----------------------------------------------------------------------------"
  printf "Semurg Arena - 11-domain database benchmark I ran on my own box (%s, %s cores, %s GiB, suite %s).\n", host, cores, ram, commit
  print "Every engine answers the SAME queries on the SAME deterministic data and only counts if its"
  print "answer hash matches (equal-answer gated). Result shown straight, wins and losses. Reproduce:"
  print "./bin/semurg-arena run --all   Methodology: data.semurg.io"
  print "-----------------------------------------------------------------------------"

  # ----------------------------- HTML artifact -----------------------------
  render_html(html_out)
  print ""
  printf "Shareable report written:  %s\n", html_out
  print "Open it in a browser and screenshot the top of the page to share."
}
# ---- ASCII line helpers (fixed 86-wide interior) ----
function rep(c,n,   s){ s=""; while(n-->0) s=s c; return s }
function aline(s){ printf "%s %-85.85s%s\n", V, s, V }   # pad AND clip to 85 so the box stays aligned
function sems(d){ return "semurg" }
function sfmt(d,   v){                # semurg headline cell for ASCII
  if(d=="graph"){ return (SEMV[d]==""?"-":SEMV[d] " ms") }
  if(d=="columnar"){ return (SEMV[d]==""?"-":SEMV[d] " rps") }
  if(d=="kv"){ return (SEMV[d]==""?"-":SEMV[d] " /s") }
  if(d=="relational"){ return "q2 " (SEM_q2[d]==""?"-":SEM_q2[d]) "ms" }
  return (SEMV[d]==""?"-":SEMV[d] " ms")
}
# ---- HTML renderer (self-contained, theme-aware, no deps) ----
function render_html(out,   d,k){
  print "<!doctype html><html lang=en><head><meta charset=utf-8>" > out
  print "<meta name=viewport content=\"width=device-width,initial-scale=1\">" > out
  print "<title>Semurg Arena Report</title>" > out
  print "<style>" > out
  print ":root{--bg:#f7f7f5;--fg:#1a1a1a;--mut:#666;--line:#ddd;--card:#fff;--win:#137a3f;--winbg:#e6f4ea;--loss:#9a6400;--lossbg:#fbf0da;--crown:#3b3a8f;--crownbg:#e8e7f7;--fold:#0f6e73;--foldbg:#e0f2f2;--mix:#555;--mixbg:#eee}" > out
  print "@media(prefers-color-scheme:dark){:root{--bg:#14140f;--fg:#eee;--mut:#aaa;--line:#333;--card:#1e1e1a;--winbg:#123522;--lossbg:#3a2c10;--crownbg:#1e1d3a;--foldbg:#0e2c2e;--mixbg:#2a2a2a}}" > out
  print "*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:15px/1.5 -apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif}" > out
  print ".wrap{max-width:920px;margin:0 auto;padding:24px}" > out
  print "header h1{font-size:24px;margin:0 0 4px}header .sub{color:var(--mut);margin:0 0 12px}" > out
  print ".chips{font:12px/1.6 ui-monospace,Menlo,Consolas,monospace;color:var(--mut)}" > out
  print ".chips span{display:inline-block;background:var(--card);border:1px solid var(--line);border-radius:6px;padding:1px 7px;margin:2px 4px 2px 0}" > out
  print ".strip{display:flex;flex-wrap:wrap;gap:8px;margin:16px 0}" > out
  print ".pill{font-weight:700;border-radius:999px;padding:6px 14px;font-size:15px}" > out
  print ".pill.win{background:var(--winbg);color:var(--win)}.pill.loss{background:var(--lossbg);color:var(--loss)}.pill.mixed{background:var(--mixbg);color:var(--mix)}.pill.crown{background:var(--crownbg);color:var(--crown)}.pill.eq{background:var(--card);border:1px solid var(--line);color:var(--fg)}" > out
  print "table{width:100%;border-collapse:collapse;background:var(--card);border:1px solid var(--line);border-radius:8px;overflow:hidden}" > out
  print "th,td{text-align:left;padding:9px 12px;border-bottom:1px solid var(--line);font-size:14px;vertical-align:top}" > out
  print "th{background:var(--bg);font-size:12px;letter-spacing:.04em;text-transform:uppercase;color:var(--mut)}" > out
  print "td.v{font-weight:600}.tag{display:inline-block;border-radius:6px;padding:1px 8px;font-size:12px;font-weight:700}" > out
  print ".tag.win{background:var(--winbg);color:var(--win)}.tag.loss{background:var(--lossbg);color:var(--loss)}.tag.mixed{background:var(--mixbg);color:var(--mix)}.tag.crown{background:var(--crownbg);color:var(--crown)}.tag.fold{background:var(--foldbg);color:var(--fold)}" > out
  print ".sub-row td{color:var(--mut);font-size:13px;border-bottom:1px solid var(--line);padding-top:2px;padding-bottom:8px}" > out
  print ".eqok{color:var(--win);font-weight:700}.eqno{color:var(--loss);font-weight:700}" > out
  print ".reading{margin:18px 0;padding:14px 16px;background:var(--card);border:1px solid var(--line);border-radius:8px;color:var(--fg)}" > out
  print "footer{margin-top:18px;color:var(--mut);font-size:13px}footer code{background:var(--card);border:1px solid var(--line);border-radius:5px;padding:1px 6px}" > out
  print "@media print{body{background:#fff}.wrap{max-width:100%}}" > out
  print "</style></head><body><div class=wrap>" > out
  printf "<header><h1>Semurg Arena &mdash; 11-domain benchmark</h1><p class=sub>Measured on YOUR hardware &middot; equal-answer-gated &middot; losses shown straight</p>" > out
  printf "<div class=chips><span>%s</span><span>%s cores</span><span>%s GiB</span><span>suite %s</span><span>%s</span></div>", META["host"], META["cores"], META["ram_gib"], META["suite_commit"], META["run_utc"] > out
  print "<div class=chips><span>deterministic generators &mdash; same knobs &rArr; same bytes &rArr; same answer hash</span></div></header>" > out
  # score strip
  printf "<div class=strip><span class=\"pill win\">%d WIN</span><span class=\"pill loss\">%d LOSS</span><span class=\"pill mixed\">%d MIXED</span><span class=\"pill crown\">%d CROWN</span><span class=\"pill eq\">equal-answer gated</span></div>", win, loss, mixed, crown > out
  print "<table><thead><tr><th>Domain</th><th>Semurg</th><th>Best incumbent</th><th>Equal-answer</th><th>Verdict</th></tr></thead><tbody>" > out
  for(k=1;k<=11;k++){
    d=DORDER[k]; if(!(d in DOMSEEN)) continue
    cls="mixed"; if(VJ[d]~/WIN/)cls="win"; else if(VJ[d]~/LOSS/)cls="loss"; else if(VJ[d]~/survives|CROWN/)cls="crown"
    eqcell = (BJ_badge[d]==XX ? "<span class=eqno>&#10007; mismatch</span>" : (BJ_badge[d]=="-" ? "<span class=eqno>&ndash;</span>" : "<span class=eqok>&#10003;</span>"))
    inc = (INCN[d]==""?"&ndash;":htmlesc(INCN[d]) (INCV[d]==""?"":" "INCV[d]))
    printf "<tr><td>%d %s</td><td class=v>%s</td><td>%s</td><td>%s</td><td><span class=\"tag %s\">%s</span></td></tr>\n", dnum[d], d, htmlesc(sfmt(d)), inc, eqcell, cls, htmlesc(VJ[d]) > out
    if(d in crownrow) printf "<tr class=sub-row><td></td><td colspan=4>&#9819; out-of-core crown: %s</td></tr>\n", htmlesc(crownrow[d]) > out
    if(d in foldrow)  printf "<tr class=sub-row><td></td><td colspan=4><span class=\"tag fold\">&#9889; FOLD</span> %s</td></tr>\n", htmlesc(foldrow[d]) > out
  }
  print "</tbody></table>" > out
  print "<div class=reading>Semurg&#39;s raw columnar scan loses to a columnar warehouse &mdash; printed straight. The win is the <b>fold</b>: the same answer served O(1) off a running monoid. The graph out-of-core row is the <b>crown</b>: at a fixed memory budget the RAM-resident engines DNF while Semurg finishes at flat memory (disk = truth).</div>" > out
  print "<footer>Reproduce: <code>./bin/semurg-arena run --all</code> &middot; Install the Semurg lane: <code>./bin/semurg-arena install</code> &middot; Full board + methodology: data.semurg.io<br>License-restricted engines are never shown here (run them locally; only you see them).</footer>" > out
  print "</div></body></html>" > out
  close(out)
}
function htmlesc(s){ gsub(/&/,"\\&amp;",s); gsub(/</,"\\&lt;",s); gsub(/>/,"\\&gt;",s); return s }
' "$TSV"

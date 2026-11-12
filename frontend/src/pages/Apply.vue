<script setup>
import { onMounted, ref, computed } from 'vue'
import { getJSON, postJSON, putJSON } from '../api'

const streams = ref([])
const streamId = ref(1)
const last = ref(null)
const attempts = ref([])
const runs = ref([])
const err = ref('')
const saveMsg = ref('')
const busy = ref(false)

const dualSettings = ref({
  policy: 'main_commit_isolate_shadow',
  policy_label: '',
  policies: [],
  shadow_fail_seqs: [],
})
const failSeqsText = ref('')

const report = computed(() => last.value?.report || {})
const dual = computed(() => report.value.dual_write || {})
const reconciliation = computed(() => {
  const d = dual.value
  if (!d.attempts) return []
  return [
    {
      item: '双写尝试',
      value: d.attempts,
      detail: '本次 run 每条 apply 事件两侧各写一次',
      check: d.attempts === (d.both_success || 0) + (d.partial_failures || 0) + (d.rollbacks || 0),
    },
    {
      item: '双侧成功',
      value: d.both_success,
      detail: '主 Sink 与旁路 Sink 均提交',
      check: true,
    },
    {
      item: '部分失败',
      value: d.partial_failures,
      detail: '主提交、旁路入隔离区（主提交+旁路隔离策略）',
      check: d.partial_failures === 0 || d.rollbacks === 0,
    },
    {
      item: '两侧回滚',
      value: d.rollbacks,
      detail: '本条两侧均未提交，游标未前进到该条（与部分失败互斥）',
      check: d.rollbacks === 0 || d.partial_failures === 0,
    },
    {
      item: '主 Sink 提交',
      value: d.main_committed,
      detail: `应等于双侧成功 ${d.both_success} + 部分失败 ${d.partial_failures}`,
      check: d.main_committed === (d.both_success || 0) + (d.partial_failures || 0),
    },
    {
      item: '旁路 Sink 提交',
      value: d.shadow_committed,
      detail: `应等于双侧成功 ${d.both_success}`,
      check: d.shadow_committed === d.both_success,
    },
  ]
})

async function loadDualSettings() {
  const s = await getJSON('/api/settings/dual-sink')
  dualSettings.value = s
  failSeqsText.value = (s.shadow_fail_seqs || []).join(',')
}

async function savePolicy() {
  saveMsg.value = ''
  err.value = ''
  try {
    await putJSON('/api/settings/dual_sink_policy', { value: dualSettings.value.policy })
    await putJSON('/api/settings/shadow_fail_seqs', { value: failSeqsText.value })
    await loadDualSettings()
    saveMsg.value = '已保存'
  } catch (e) {
    err.value = String(e.message || e)
  }
}

async function refreshRuns() {
  runs.value = await getJSON('/api/runs')
}

async function loadAttempts(runId) {
  if (!runId) {
    attempts.value = []
    return
  }
  const data = await getJSON(`/api/runs/${runId}/dual-write`)
  attempts.value = data.attempts
}

async function runApply() {
  busy.value = true
  err.value = ''
  saveMsg.value = ''
  try {
    last.value = await postJSON('/api/apply', { stream_id: streamId.value })
    streams.value = await getJSON('/api/streams')
    await loadAttempts(last.value.id)
    await refreshRuns()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

function rowTag(outcome) {
  if (outcome === 'both_committed') return 'ok'
  if (outcome === 'main_committed_shadow_isolated') return 'warn'
  return 'danger'
}

onMounted(async () => {
  streams.value = await getJSON('/api/streams')
  if (streams.value.length) streamId.value = streams.value[0].id
  await loadDualSettings()
  await refreshRuns()
})
</script>

<template>
  <div class="page">
    <h1>应用台</h1>
    <p class="muted">
      每条 apply 事件同时写主 Sink 与旁路 Sink。旁路失败时按所选策略互斥取舍：
      两侧回滚且游标停，或主提交/旁路隔离且游标前进。报告计数与两侧 Sink、隔离区、游标同源。
    </p>

    <div class="panel">
      <h3>双 Sink 部分失败策略</h3>
      <p v-if="dualSettings.policy_label" class="muted">
        当前生效：<span class="tag warn">{{ dualSettings.policy_label }}</span>
      </p>
      <div class="row-actions">
        <label>策略
          <select v-model="dualSettings.policy">
            <option v-for="p in dualSettings.policies" :key="p.value" :value="p.value">{{ p.label }}</option>
          </select>
        </label>
        <label>旁路故障注入 seq（逗号分隔，留空=不注入）
          <input v-model="failSeqsText" placeholder="例如 2,4" />
        </label>
        <button class="ghost" @click="savePolicy">保存策略</button>
        <span v-if="saveMsg" class="tag ok">{{ saveMsg }}</span>
      </div>
    </div>

    <div class="row-actions panel">
      <label>流
        <select v-model.number="streamId">
          <option v-for="s in streams" :key="s.id" :value="s.id">{{ s.id }} · {{ s.name }}（游标 {{ s.apply_cursor }}）</option>
        </select>
      </label>
      <button :disabled="busy" @click="runApply">{{ busy ? '应用中…' : '执行 Apply' }}</button>
    </div>
    <p v-if="err" class="tag danger">{{ err }}</p>

    <div class="panel" v-if="last">
      <h3>最近一次 · run #{{ last.id }}</h3>
      <p>
        <span class="tag" :class="last.status === 'succeeded' ? 'ok' : 'warn'">{{ last.status }}</span>
        · loaded {{ last.loaded_count }} / applied {{ last.applied_count }} /
        quarantined {{ last.quarantined_count }} / skipped {{ last.skipped_count }}
        · cursor {{ last.cursor_before }} → {{ last.cursor_after }}
        <span class="tag" :class="report.cursor_advanced ? 'warn' : 'danger'">
          游标{{ report.cursor_advanced ? `前进 ${last.cursor_before}→${last.cursor_after}` : '未前进' }}
        </span>
        <span v-if="report.halted_at_seq" class="tag danger">
          止于 seq {{ report.halted_at_seq }}（{{ report.halt_reason }}，该条未提交）
        </span>
      </p>
      <p class="muted">{{ report.cursor_policy }}</p>

      <h4>双写对账</h4>
      <table>
        <thead>
          <tr><th>口径</th><th>数量</th><th>说明</th><th>自洽</th></tr>
        </thead>
        <tbody>
          <tr v-for="row in reconciliation" :key="row.item">
            <td>{{ row.item }}</td>
            <td><strong>{{ row.value }}</strong></td>
            <td class="muted">{{ row.detail }}</td>
            <td><span class="tag" :class="row.check ? 'ok' : 'danger'">{{ row.check ? '✓' : '✗' }}</span></td>
          </tr>
          <tr>
            <td>主 Sink 当前行数</td>
            <td><strong>{{ report.sink_count_after }}</strong></td>
            <td class="muted">累计态（delete 后行消失）</td>
            <td><span class="tag ok">累计</span></td>
          </tr>
          <tr>
            <td>旁路 Sink 当前行数</td>
            <td><strong>{{ report.shadow_sink_count_after }}</strong></td>
            <td class="muted">累计态，与主 Sink 分别查看</td>
            <td><span class="tag ok">累计</span></td>
          </tr>
          <tr>
            <td>隔离区当前行数</td>
            <td><strong>{{ report.quarantine_count_after }}</strong></td>
            <td class="muted">含缺口与旁路失败（shadow_sink_failed）</td>
            <td><span class="tag ok">累计</span></td>
          </tr>
        </tbody>
      </table>
      <p v-if="dual.failed_seqs && dual.failed_seqs.length" class="muted">
        失败 seq：{{ dual.failed_seqs.join(', ') }}
      </p>

      <h4>本次双写明细</h4>
      <table>
        <thead>
          <tr><th>seq</th><th>op</th><th>entity</th><th>outcome</th><th>主</th><th>旁路</th><th>游标前进</th><th>policy</th><th>reason</th></tr>
        </thead>
        <tbody>
          <tr v-for="a in attempts" :key="a.id">
            <td>{{ a.seq }}</td>
            <td>{{ a.op }}</td>
            <td>{{ a.entity_key }}</td>
            <td><span class="tag" :class="rowTag(a.outcome)">{{ a.outcome }}</span></td>
            <td>{{ a.main_committed ? '✓' : '✗' }}</td>
            <td>{{ a.shadow_committed ? '✓' : '✗' }}</td>
            <td>{{ a.cursor_advanced ? '是' : '否' }}</td>
            <td class="muted">{{ a.policy }}</td>
            <td class="muted">{{ a.reason }}</td>
          </tr>
          <tr v-if="!attempts.length"><td colspan="9" class="muted">本次无 apply 事件</td></tr>
        </tbody>
      </table>
    </div>

    <div class="panel">
      <h3>历史 Runs</h3>
      <table>
        <thead>
          <tr><th>ID</th><th>流</th><th>状态</th><th>loaded</th><th>applied</th><th>quarantine</th><th>skipped</th><th>双写</th><th>cursor</th><th>时间</th></tr>
        </thead>
        <tbody>
          <tr v-for="r in runs" :key="r.id">
            <td>{{ r.id }}</td>
            <td>{{ r.stream_id }}</td>
            <td>{{ r.status }}</td>
            <td>{{ r.loaded_count }}</td>
            <td>{{ r.applied_count }}</td>
            <td>{{ r.quarantined_count }}</td>
            <td>{{ r.skipped_count }}</td>
            <td class="muted">
              <template v-if="r.report && r.report.dual_write">
                尝试 {{ r.report.dual_write.attempts }} · 成功 {{ r.report.dual_write.both_success }} ·
                部分 {{ r.report.dual_write.partial_failures }} · 回滚 {{ r.report.dual_write.rollbacks }}
              </template>
            </td>
            <td>{{ r.cursor_before }}→{{ r.cursor_after }}</td>
            <td>{{ r.created_at }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<script setup>
import { onMounted, ref } from 'vue'
import { getJSON } from '../api'
const mainRows = ref([])
const shadowRows = ref([])
const view = ref('both')
const err = ref('')
async function load() {
  err.value = ''
  try {
    ;[mainRows.value, shadowRows.value] = await Promise.all([
      getJSON('/api/sink'),
      getJSON('/api/sink/shadow'),
    ])
  } catch (e) {
    err.value = String(e.message || e)
  }
}
onMounted(load)
</script>
<template>
  <div class="page">
    <h1>双 Sink</h1>
    <p class="muted">
      应用成功时同时写入主 Sink 与旁路 Sink，两张表分别展示，均可按 seq 与实体键对齐。
      delete 后行消失属正常终态。
    </p>
    <div class="row-actions panel">
      <button class="ghost" :class="{ active: view === 'both' }" @click="view = 'both'">并排</button>
      <button class="ghost" @click="view = 'main'">主 Sink（{{ mainRows.length }}）</button>
      <button class="ghost" @click="view = 'shadow'">旁路 Sink（{{ shadowRows.length }}）</button>
      <button @click="load">刷新</button>
    </div>
    <p v-if="err" class="tag danger">{{ err }}</p>

    <div class="sink-grid">
      <div class="panel" v-if="view !== 'shadow'">
        <h3>主 Sink · sink_records <span class="tag ok">{{ mainRows.length }} 行</span></h3>
        <table>
          <thead>
            <tr><th>流</th><th>entity_key</th><th>last_seq</th><th>last_event_ts</th><th>payload</th><th>updated</th></tr>
          </thead>
          <tbody>
            <tr v-for="r in mainRows" :key="r.id">
              <td>{{ r.stream_id }}</td>
              <td>{{ r.entity_key }}</td>
              <td>{{ r.last_seq }}</td>
              <td>{{ r.last_event_ts }}</td>
              <td><pre class="mono">{{ JSON.stringify(r.payload) }}</pre></td>
              <td>{{ r.updated_at }}</td>
            </tr>
            <tr v-if="!mainRows.length"><td colspan="6" class="muted">暂无主 Sink 记录</td></tr>
          </tbody>
        </table>
      </div>
      <div class="panel" v-if="view !== 'main'">
        <h3>旁路 Sink · sink_shadow_records <span class="tag warn">{{ shadowRows.length }} 行</span></h3>
        <table>
          <thead>
            <tr><th>流</th><th>entity_key</th><th>last_seq</th><th>last_event_ts</th><th>payload</th><th>updated</th></tr>
          </thead>
          <tbody>
            <tr v-for="r in shadowRows" :key="r.id">
              <td>{{ r.stream_id }}</td>
              <td>{{ r.entity_key }}</td>
              <td>{{ r.last_seq }}</td>
              <td>{{ r.last_event_ts }}</td>
              <td><pre class="mono">{{ JSON.stringify(r.payload) }}</pre></td>
              <td>{{ r.updated_at }}</td>
            </tr>
            <tr v-if="!shadowRows.length"><td colspan="6" class="muted">暂无旁路 Sink 记录</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>
</template>

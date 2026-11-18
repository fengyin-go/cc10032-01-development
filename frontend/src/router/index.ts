import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'
import WindowMetricView from '@/views/WindowMetricView.vue'
import SinkDetailView from '@/views/SinkDetailView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/window_metric_result', name: 'window-metric', component: WindowMetricView },
    { path: '/sink_output/:entryId/detail', name: 'sink-detail', component: SinkDetailView, props: true },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router

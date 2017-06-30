<template>
  <canvas class="canvas-gauges"></canvas>
</template>

<script>
import { RadialGauge } from 'canvas-gauges'

export default {

  props: {
    value: Number,
    options: {
      type: Object,
      default: () => ({})
    }
  },

  data () {
    return {
      chart: null
    }
  },

  mounted () {
    if (this.value) this.options.value = this.value
    this.options.renderTo = this.$el
    this.options.colorNumbers = 'red'
    this.options.colorPlate = '#222'
    this.options.height = "200"
    this.options.width = "200"
    this.options.minValue = "0"
    this.options.maxValue = "255"
    this.options.highlights = "0"
    this.options.exactTicks= "false"
    // this.options.majorTicks = "0, 15, 30, 45, 60, 75, 90, 105, 120, 135, 150, 165, 180, 195,210, 225, 240, 255"
    // this.options.minorTicks = "10"
    this.chart = new RadialGauge(this.options).draw()
  },

  beforeDestroy() {
    this.chart.destroy()
  },

  watch: {
    value (val) {
      this.chart.value = val
    }
  }

}
</script>

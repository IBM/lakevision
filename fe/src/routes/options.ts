import { ScaleTypes } from '@carbon/charts-svelte'
export default {
  title: 'Data Additions',
  axes: {
    left: {
      title: 'Added Records',
      mapsTo: 'added-records',
      scaleType: ScaleTypes.LOG, 
      
    },
    right: {
      title: 'Added Bytes',
      mapsTo: 'added-files-size',      
      scaleType: ScaleTypes.LOG,     
    },
    bottom: {
      mapsTo: 'committed_at',
      scaleType: 'time'
    }
  },
  height: '400px',
  accessibility: {
    svgAriaLabel: 'Data Additions'
  },
  zoomBar: {
    top: {
      enabled: true
    }
  }
}
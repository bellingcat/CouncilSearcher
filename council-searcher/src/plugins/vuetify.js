/**
 * plugins/vuetify.js
 *
 * Framework documentation: https://vuetifyjs.com`
 */

// Styles
import 'vuetify/styles'

// Composables
import { createVuetify } from 'vuetify'
import { aliases, mdi } from 'vuetify/iconsets/mdi-svg'
import { mdiVideo } from '@mdi/js'

// Labs
import { VDateInput } from 'vuetify/labs/VDateInput'

// https://vuetifyjs.com/en/introduction/why-vuetify/#feature-guides
export default createVuetify({
  icons: {
    defaultSet: 'mdi',
    aliases: {
      ...aliases,
      mdiVideo: mdiVideo,
    },
    sets: {
      mdi,
    },
  },
  theme: {
    defaultTheme: 'dark',
  },
  components: {
    VDateInput,
  },
})

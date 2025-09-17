<template>
  <v-card class="mb-4 pb-4" outlined>
    <v-card-title class="text-left">{{ result.title }}</v-card-title>
    <v-card-text
      class="text-left"
      v-html="highlightedSnippet"
    />
    <v-card-subtitle class="text-left">
      {{ result.datetime }} - {{ result.start_time }}
    </v-card-subtitle>
    <v-card-actions class="d-flex justify-space-between">
      <v-btn
        :href="result.link"
        prepend-icon="$mdiVideo"
        target="_blank"
        text
        variant="tonal"
      >
        Watch the video
      </v-btn>
      <v-chip
        class="ma-2"
        color="primary"
        outlined
        small
        text
      >
        {{ result.authority }}
      </v-chip>
    </v-card-actions>
  </v-card>
</template>

<script>
  export default {
    props: {
      result: {
        type: Object,
        required: true,
      },
    },
    computed: {
      highlightedSnippet () {
        // Use a regular expression to find text within square brackets
        return this.result.snippet.replace(
          /\[(.*?)\]/g,
          '<span class="highlight">$1</span>'
        );
      },
    },
  };
</script>

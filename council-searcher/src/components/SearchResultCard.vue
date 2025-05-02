<template>
    <v-card class="mb-4 pb-4" outlined>
        <v-card-title class="text-left">{{ result.title }}</v-card-title>
        <v-card-text
            class="text-left"
            v-html="highlightedSnippet"
        ></v-card-text>
        <v-card-subtitle class="text-left">
            {{ result.datetime }} - {{ result.start_time }}
        </v-card-subtitle>
        <v-card-actions>
            <v-btn
                prepend-icon="mdi-video"
                :href="result.link"
                target="_blank"
                variant="tonal"
                text
            >
                Watch the video
            </v-btn>
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
        highlightedSnippet() {
            // Use a regular expression to find text within square brackets
            return this.result.snippet.replace(
                /\[(.*?)\]/g,
                '<span class="highlight">$1</span>'
            );
        },
    },
};
</script>

<style scoped></style>

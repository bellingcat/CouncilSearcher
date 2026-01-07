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
        <v-card-actions class="d-flex justify-space-between align-center">
            <div class="d-flex" style="gap: 16px;">
                <v-btn
                    prepend-icon="$mdiVideo"
                    :href="result.link"
                    target="_blank"
                    variant="tonal"
                    text
                >
                    Watch the video
                </v-btn>
                <v-btn
                    @click="downloadTranscript"
                    prepend-icon="$mdiDownload"
                    variant="tonal"
                    text
                >
                    Download Transcript
                </v-btn>   
            </div>
            <v-chip class="ma-2" color="primary" text small outlined>
                {{ result.authority }}
            </v-chip>
        </v-card-actions>
             
    </v-card>
</template>


<script>
import axios from 'axios';

const API_BASE = import.meta.env.VITE_API_BASEURL;
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
    methods: {

        async downloadTranscript() {
    try {
        const response = await axios.get(
            `${API_BASE}/meetings/download_transcript/${this.result.uid}`,
            { responseType: 'blob' }
        );
        
        let authority = this.result.authority.charAt(0).toUpperCase() + this.result.authority.slice(1);
        let title = this.result.title.replace(/[^a-zA-Z0-9\s]/g, '').trim().split(' ').slice(0, 3).join('');
        
        // date formating
        const date = new Date(this.result.datetime);
        const day = String(date.getDate()).padStart(2, '0');
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const year = String(date.getFullYear());
        const date_str = `${year}-${month}-${day}`;
        
        const filename = `${authority}-${title}-[${date_str}].txt`;

        const url = window.URL.createObjectURL(new Blob([response.data]));
        const link = document.createElement('a');
        link.href = url;
        link.setAttribute('download', filename);
        document.body.appendChild(link);
        link.click();
        link.remove();
        window.URL.revokeObjectURL(url);
    } catch (error) {
        console.error('Error downloading transcript:', error);
        alert('Failed to download transcript. Please try again.');
    }
}
    }
};
</script>

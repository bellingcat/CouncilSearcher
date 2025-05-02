<template>
    <v-container class="fill-height" max-width="900">
        <v-responsive class="align-center text-center overflow-visible pa-4">
            <v-row class="mb-4">
                <v-col
                    cols="12"
                    class="mb-4 justify-center align-center text-center"
                >
                    <v-img
                        class="mb-0 mt-4"
                        height="3em"
                        src="@/assets/bellingcat.svg"
                    />
                    <h1 class="mb-6 py-2 font-weight-bold">
                        Council Meeting Transcript Search
                    </h1>
                    <a href="https://www.bellingcat.com">
                        <img
                            alt="Bellingcat logo: Discover Bellingcat"
                            src="https://img.shields.io/badge/Discover%20Bellingcat-%20?style=for-the-badge&logo=data%3Aimage%2Fpng%3Bbase64%2CiVBORw0KGgoAAAANSUhEUgAAAA4AAAAYCAYAAADKx8xXAAABhGlDQ1BJQ0MgcHJvZmlsZQAAKJF9kT1Iw0AcxV9TS0UqDnZQEcxQneyiIo6likWwUNoKrTqYXPoFTRqSFBdHwbXg4Mdi1cHFWVcHV0EQ%2FABxdnBSdJES%2F5cUWsR4cNyPd%2Fced%2B8AoVllqtkTA1TNMtKJuJjLr4rBVwQwhhBEDEvM1JOZxSw8x9c9fHy9i%2FIs73N%2Fjn6lYDLAJxLHmG5YxBvEs5uWznmfOMzKkkJ8Tjxp0AWJH7kuu%2FzGueSwwDPDRjY9TxwmFktdLHcxKxsq8QxxRFE1yhdyLiuctzir1Tpr35O%2FMFTQVjJcpzmKBJaQRIo6klFHBVVYiNKqkWIiTftxD%2F%2BI40%2BRSyZXBYwcC6hBheT4wf%2Fgd7dmcXrKTQrFgcCLbX%2BMA8FdoNWw7e9j226dAP5n4Err%2BGtNYO6T9EZHixwBA9vAxXVHk%2FeAyx1g6EmXDMmR%2FDSFYhF4P6NvygODt0Dfmttbex%2BnD0CWulq%2BAQ4OgYkSZa97vLu3u7d%2Fz7T7%2BwHEU3LHAa%2FQ6gAAAAZiS0dEAAAAAAAA%2BUO7fwAAAAlwSFlzAAAuIwAALiMBeKU%2FdgAAAAd0SU1FB%2BgFHwwiMH4odB4AAAAZdEVYdENvbW1lbnQAQ3JlYXRlZCB3aXRoIEdJTVBXgQ4XAAAA50lEQVQ4y82SvWpCQRCFz25ERSJiCNqlUiS1b5AuEEiZIq1NOsGXCKms0wXSp9T6dskDiFikyiPc%2FrMZyf3FXSGQ0%2BzuzPl2ZoeVKgQ0gQ2wBVpVHlcDkjM5V%2FJ5nag6sJ%2FZX%2Bh%2FC7gEhqeAFKf7p1M9aB3b5oN1OomB7g1axUBPBr3GQHODHmOgqUF3MZAzKI2d4LWBV4H%2BMXDuJd1a7Cew1k7SwksaHC4LqNaw7aeX9GWHXkC1G1sTAS17Y3Kk2lnp4wNLiz0DrgLq8qt2MfmSSabAO%2FBBXp26dtrADPjOmN%2BAUdG7B3cE61l5hOZiAAAAAElFTkSuQmCC&logoColor=%23fff&color=%23000"
                        />
                    </a>
                    <a href="https://discord.gg/bellingcat">
                        <img
                            alt="Discord logo: Join our community"
                            src="https://img.shields.io/badge/Join%20our%20community-%20?style=for-the-badge&logo=discord&logoColor=%23fff&color=%235865F2"
                        />
                    </a>
                    <a href="https://www.bellingcat.com/donate/">
                        <img
                            alt="Heart icon: Support our work"
                            src="https://img.shields.io/badge/Support%20Our%20Work-%20?style=for-the-badge&logo=data%3Aimage%2Fsvg%2Bxml%3Bbase64%2CPD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiIHN0YW5kYWxvbmU9Im5vIj8%2BCjxzdmcKICAgd2lkdGg9IjExMCIKICAgaGVpZ2h0PSIxMTAiCiAgIHZlcnNpb249IjEuMSIKICAgaWQ9InN2ZzEiCiAgIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIKICAgeG1sbnM6c3ZnPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyI%2BCiAgPGRlZnMKICAgICBpZD0iZGVmczEiIC8%2BCiAgPHBhdGgKICAgICBkPSJtIDgxLjU3LDguMDA3MjY2OCBjIC0xMS42NiwwIC0yMS44MSw2Ljk3MDAwMDIgLTI2LjM1LDE3LjA2MDAwMDIgQyA1MC42OCwxNC45ODcyNjcgNDAuNTMsOC4wMDcyNjY4IDI4Ljg3LDguMDA3MjY2OCAxMi45NSw4LjAwNzI2NjggMCwyMC45NjcyNjcgMCwzNi44ODcyNjcgYyAwLDM1LjkgNTEuNzksNjUuNDYwMDAzIDU0LDY2LjcwMDAwMyAwLjM4LDAuMjEgMC43OSwwLjMyIDEuMjEsMC4zMiAwLjQyLDAgMC44NCwtMC4xMSAxLjIxLC0wLjMyIDIuMiwtMS4yNCA1NC4wMSwtMzAuODAwMDAzIDU0LjAxLC02Ni43MDAwMDMgMC4wMSwtMTUuOTIgLTEyLjk0LC0yOC44ODAwMDAyIC0yOC44NiwtMjguODgwMDAwMiB6IgogICAgIHN0eWxlPSJmaWxsOiNmZmZmZmY7ZmlsbC1vcGFjaXR5OjEiCiAgICAgaWQ9InBhdGgxIiAvPgo8L3N2Zz4K&logoSize=auto&color=%23A94ECA"
                        />
                    </a>
                </v-col>
            </v-row>
            <v-row>
                <v-card class="px-4 py-2 mb-2" color="surface-variant">
                    <p>
                        This is a local democracy reporting tool that enables
                        researchers to search auto-generated transcripts of
                        Council meetings for different authorities across the UK
                        and Ireland.
                    </p>
                </v-card>
            </v-row>
            <v-row>
                <v-card
                    class="px-4 py-2 mt-4 mb-2"
                    title="Filter by Authority"
                    color="surface"
                >
                    <p class="text">
                        Select the authorities of interest. The brackets
                        indicate the number of transcripts available.
                    </p>
                    <v-text-field
                        v-model="authorityFilter"
                        label="Filter Authorities"
                        outlined
                        clearable
                        class="mb-0 mt-2"
                        density="compact"
                    />
                    <v-container
                        class="mt-0"
                        fluid
                        style="max-height: 100px; overflow-y: auto"
                    >
                        <v-row>
                            <v-col
                                v-for="(
                                    count, authority
                                ) in filteredAuthorities"
                                :key="authority"
                                cols="auto"
                                class="px-1 py-0"
                            >
                                <v-checkbox
                                    v-model="selectedAuthorities"
                                    density="compact"
                                    :label="`${authority} (${count})`"
                                    :value="authority"
                                    hide-details="true"
                                />
                            </v-col>
                        </v-row>
                    </v-container>
                </v-card>
            </v-row>
            <v-row>
                <v-text-field
                    v-model="searchQuery"
                    label="Search"
                    outlined
                    clearable
                    class="mt-2 mb-0"
                    @keyup.enter="performSearch"
                    @click:clear="results = []"
                >
                    <template #append-inner>
                        <v-btn
                            variant="flat"
                            color="primary"
                            @click="performSearch"
                        >
                            Search
                        </v-btn>
                    </template>
                </v-text-field>
            </v-row>
            <v-row class="mt-0">
                <v-col cols="12" md="6">
                    <v-date-input
                        v-model="startDate"
                        label="Start Date"
                        outlined
                        clearable="true"
                        density="compact"
                        :display-format="formatAsIso"
                        placeholder="yyyy-mm-dd"
                        @update:model-value="performSearch"
                    />
                </v-col>
                <v-col cols="12" md="6">
                    <v-date-input
                        v-model="endDate"
                        label="End Date"
                        outlined
                        clearable="true"
                        :min="startDate"
                        density="compact"
                        :display-format="formatAsIso"
                        placeholder="yyyy-mm-dd"
                        @update:model-value="performSearch"
                    />
                </v-col>
            </v-row>
            <v-row v-if="results.length" class="my-0 py-0">
                <v-col class="ma-0 pa-0" cols="auto">
                    <div class="text-h4">Search Results</div>
                </v-col>
                <v-spacer />
                <v-col class="ma-0 pa-0" cols="auto">
                    <v-select
                        v-model="sortOption"
                        :items="sortOptions"
                        label="Sort by"
                        outlined
                        class="mt-2 mb-2"
                        @change="sortResults"
                        hide-details="true"
                        density="compact"
                    />
                </v-col>
            </v-row>
            <v-row v-if="results.length" class="mt-2">
                <template v-for="result in sortedResults" :key="result.link">
                    <SearchResultCard :result="result" />
                </template>
            </v-row>
        </v-responsive>
    </v-container>
</template>

<script setup>
import { ref, computed } from "vue";
import { useDate } from "vuetify";
import axios from "axios";

const searchQuery = ref("");
const results = ref([]);
const selectedAuthorities = ref([]);
const authorities = ref({});
const sortOption = ref("Best Match");
const sortOptions = [
    "Best Match",
    "Date (newest first)",
    "Date (oldest first)",
];
const startDate = ref(null);
const endDate = ref(null);
const adapter = useDate();
function formatAsIso(date) {
    return adapter.toISO(date);
}

const authorityFilter = ref("");

const filteredAuthorities = computed(() => {
    if (!authorityFilter.value) {
        return authorities.value;
    }
    const filter = authorityFilter.value.toLowerCase();
    return Object.fromEntries(
        Object.entries(authorities.value).filter(([key]) =>
            key.toLowerCase().includes(filter)
        )
    );
});

const sortedResults = computed(() => {
    if (sortOption.value === "Best Match") {
        return [...results.value].sort((a, b) => b.rank - a.rank);
    } else if (sortOption.value === "Date (newest first)") {
        return [...results.value].sort((a, b) => b.unixtime - a.unixtime);
    } else if (sortOption.value === "Date (oldest first)") {
        return [...results.value].sort((a, b) => a.unixtime - b.unixtime);
    }
    return results.value;
});

const fetchAuthorities = async () => {
    try {
        const response = await axios.get(
            "http://127.0.0.1:5000/transcript_counts_by_authority"
        );
        authorities.value = response.data;
    } catch (error) {
        console.error("Error fetching authorities:", error);
    }
};

const performSearch = async () => {
    try {
        const authorityParams = selectedAuthorities.value
            .map((authority) => `authority=${encodeURIComponent(authority)}`)
            .join("&");
        const dateParams = [
            startDate.value
                ? `startdate=${encodeURIComponent(
                      formatAsIso(startDate.value)
                  )}`
                : "",
            endDate.value
                ? `enddate=${encodeURIComponent(formatAsIso(endDate.value))}`
                : "",
        ]
            .filter(Boolean)
            .join("&");
        const queryParams = `query=${encodeURIComponent(searchQuery.value)}${
            authorityParams ? `&${authorityParams}` : ""
        }${dateParams ? `&${dateParams}` : ""}`;
        const response = await axios.get(
            `http://127.0.0.1:5000/search?${queryParams}`
        );
        results.value = response.data;
    } catch (error) {
        console.error("Error fetching search results:", error);
    }
};

const sortResults = () => {
    // Trigger reactivity for sorting
    sortedResults.value;
};

fetchAuthorities();
</script>

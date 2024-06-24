<template>
    <div>
        <data-list
            :setting="setting"
        >
        </data-list>
    </div>
</template>

<script>
import DataList
    from '../../components/render/DataList'
import setting
    from '../../data/render/demo.json'

export default {
    name: "view-page-name",
    components: {
        DataList
    },
    mounted() {

    },
    data() {
        return {
            setting: setting
        }
    },
    methods: {}
}
</script>
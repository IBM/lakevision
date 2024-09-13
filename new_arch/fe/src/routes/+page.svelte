
<script>
    import { env } from '$env/dynamic/public';
    import { Tile, Content, Tabs, Tab, TabContent } from "carbon-components-svelte";
    import { selectedNamespce } from '$lib/stores';
    import { selectedTable } from '$lib/stores';
    import Table from '../lib/components/Table.svelte';

    let namespace;
    let table;
    let error = "";
    $: {
        selectedNamespce.subscribe(value => {namespace = value; });
        selectedTable.subscribe(value => {table = value; });
    }

    let partitions = [];
    let partitions_loading = true;
    let snapshots = [];
    let snapshots_loading = true;
    let sample_data = [];
    let sample_data_loading = true;

    async function get_data(table_id, feature){
        let loading = true;
        if(!table_id || table_id == null || table_id == "." || !table) {
            loading = false;      
            return;
        }
        try{            
            const res = await fetch(`${env.PUBLIC_API_SERVER}/tables/${table_id}/${feature}`);            
            if (res.ok) {
                const data = await res.json();                                   
                return JSON.parse(data);                
            }
            else{
                console.error("Failed to fetch data:", res.statusText);
                error = res.statusText;
            }
        } 
        finally {
            loading = false; 
        }  
    }            
    
    $: (async () => {
        try {        
            partitions_loading = true; 
            partitions = await get_data(namespace+"."+table, "partitions");  // Wait for the promise to resolve
            partitions_loading = false;  
        } catch (err) {
            error = err.message; 
            partitions_loading = false;  
        }
        try {
            snapshots_loading = true;        
            snapshots = await get_data(namespace+"."+table, "snapshots");  
            snapshots_loading = false;  
        } catch (err) {
            error = err.message; 
            snapshots_loading = false;  
        }
        try {
            sample_data_loading = true;          
            sample_data = await get_data(namespace+"."+table, "sample");  
            sample_data_loading = false;  
        } catch (err) {
            error = err.message; 
            sample_data_loading = false;  
        }
    })();
 
</script>

<Content>
    <Tile><h4>Namespace: {namespace}</h4></Tile>
    <Tile><h4>Table:     {table}</h4></Tile>
    <br />
    <Tabs>
        <Tab label="Partitions" />
        <Tab label="Snapshots" />
        <Tab label="Sample Data" />
        <svelte:fragment slot="content">
        <TabContent><br/>
            <Table fetched_data={partitions} loading={partitions_loading}/>
        </TabContent>

        <TabContent><br/>
            <Table fetched_data={snapshots} loading={snapshots_loading}/>            
        </TabContent>

        <TabContent><br/>
            <Table fetched_data={sample_data} loading={sample_data_loading}/>                  
            
        </TabContent>
        </svelte:fragment>
      </Tabs>
    
  </Content>




<script>
    import { env } from '$env/dynamic/public';    
    import { Tile, Content, Tabs, Tab, TabContent, Grid, Row, Column, CopyButton } from "carbon-components-svelte";
    import { selectedNamespce } from '$lib/stores';
    import { selectedTable } from '$lib/stores';
    import { onMount } from 'svelte';
    import Table from '../lib/components/Table.svelte';
    import JsonTable from '../lib/components/JsonTable.svelte';

    let namespace;
    let table;
    let error = "";
    let url = "";
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
    let schema = [];
    let schema_loading = true;
    let summary = [];
    let summary_loading = true;
    let partition_specs = [];
    let partition_specs_loading = true;
    let properties = [];
    let properties_loading = true;

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
            summary_loading = true;          
            summary = await get_data(namespace+"."+table, "summary");  
            summary_loading = false;              
        } catch (err) {
            error = err.message; 
            summary_loading = false;  
        }
        try {
            properties_loading = true;          
            properties = await get_data(namespace+"."+table, "properties");  
            properties_loading = false;  
        } catch (err) {
            error = err.message; 
            properties_loading = false;  
        }
        try {
            schema_loading = true;          
            schema = await get_data(namespace+"."+table, "schema");  
            schema_loading = false;  
        } catch (err) {
            error = err.message; 
            schema_loading = false;  
        }        
        try {
            partition_specs_loading = true;          
            partition_specs = await get_data(namespace+"."+table, "partition-specs");  
            partition_specs_loading = false; 
        } catch (err) {
            error = err.message; 
            partition_specs_loading = false;  
        }
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
 
    function set_copy_url(){
        url = window.location.origin;
        url = url+"/?namespace="+namespace+"&table="+table;
    }
</script>

<Content>    
    <Tile><h4>Namespace: {namespace} </h4> <p align="right"> <CopyButton text={url} on:click={set_copy_url} iconDescription="Copy table link" feedback="Table link copied" /></p>
        <h4>Table:     {table}</h4>
    </Tile>    
  
    <br />
    <Tabs>
        <Tab label="Summary" />
        <Tab label="Partitions" />
        <Tab label="Snapshots" />
        <Tab label="Sample Data" />
        <svelte:fragment slot="content">
            <TabContent><br/>
                <Grid>
                    <Row>
                      <Column aspectRatio="2x1">
                        
                        <br />
                        Summary
                        <br />                        
                        <br />
                        {#if !summary_loading && summary}
                            <JsonTable jsonData={summary}></JsonTable>                                                     
                        {/if}
                        <br />
                        <br />
                        Properties
                        <br />
                        <br />
                        {#if !properties_loading && properties}
                            <JsonTable jsonData={properties}></JsonTable>                                                     
                        {/if}
                     </Column>
                      <Column aspectRatio="2x1">
                        <br />
                        Schema
                        <br />
                        <br />
                        <Table fetched_data={schema} loading={schema_loading} table_title={namespace}.{table}/>
                        </Column>
                    </Row>
                  </Grid>
                
            </TabContent>
            <TabContent><br/>                                         
                <Table fetched_data={partitions} loading={partitions_loading} table_title={namespace}.{table}/>
            </TabContent>

            <TabContent><br/>
                <Table fetched_data={snapshots} loading={snapshots_loading} table_title={namespace}.{table}/>            
            </TabContent>

            <TabContent><br/>
                <Table fetched_data={sample_data} loading={sample_data_loading} table_title={namespace}.{table}/>                  
                
            </TabContent>
        </svelte:fragment>
      </Tabs>
    
  </Content>




<script>
    import { env } from '$env/dynamic/public';    
    import { Tile, Content, Tabs, Tab, TabContent, Grid, Row, Column, CopyButton } from "carbon-components-svelte";
    import { selectedNamespce } from '$lib/stores';
    import { selectedTable } from '$lib/stores';
    import JsonTable from '../lib/components/JsonTable.svelte';
    import { Loading } from 'carbon-components-svelte';
    import { BarChartSimple } from '@carbon/charts-svelte'    
    import '@carbon/charts-svelte/styles.css'
    import options from './options'        	
    import VirtualTable from '../lib/components/VirtTable3.svelte';

    let namespace;
    let table;
    let error = "";
    let url = "";
    let pageSessionId = Date.now().toString(36) + Math.random().toString(36).substring(2);       

    $: {
        selectedNamespce.subscribe(value => {namespace = value; });
        selectedTable.subscribe(value => {table = value; });
    }

    let partitions = [];
    let partitions_loading = false;
    let snapshots = [];
    let snapshots_loading = false;
    let sample_data = [];
    let sample_data_loading = false;
    let schema = [];
    let schema_loading = false;
    let summary = [];
    let summary_loading = false;
    let partition_specs = [];
    let partition_specs_loading = false;
    let properties = [];
    let properties_loading = false;
    let data_change = [];
    let data_change_loading = false;

    async function get_data(table_id, feature){
        let loading = true;
        if(!table_id || table_id == null || table_id == "." || !table) {
            loading = false;      
            return;
        }
        try{            
            const res = await fetch(
                '/api/tables/${table_id}/${feature}',
                {
                    //method: 'GET',
                    headers: {
                        'Content-Type': 'application/json',
                        'X-Page-Session-ID': pageSessionId, // Include the session ID in the headers
                    },
                    //queryParams: { table_id: tableId },
                }
            );            
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
    
    let selected = 0; 
    $: reset(table);

    $: (async () => {        
        if( table === '') return;
        try {
            if(selected==0 ){
                summary_loading = true;          
                summary = await get_data(namespace+"."+table, "summary");  
                summary_loading = false;   
            }           
        } catch (err) {
            error = err.message; 
            summary_loading = false;  
        }
        try {
            if(selected==0 ){
                properties_loading = true;          
                properties = await get_data(namespace+"."+table, "properties");  
                properties_loading = false;  
            }
        } catch (err) {
            error = err.message; 
            properties_loading = false;  
        }
        try {
            if(selected==0 ){
                schema_loading = true;          
                schema = await get_data(namespace+"."+table, "schema");  
                schema_loading = false;  
            }
        } catch (err) {
            error = err.message; 
            schema_loading = false;  
        }        
        try {
            if(selected==0 ){
                partition_specs_loading = true;          
                partition_specs = await get_data(namespace+"."+table, "partition-specs");  
                partition_specs_loading = false; 
            }
        } catch (err) {
            error = err.message; 
            partition_specs_loading = false;  
        }
        try {        
            if(selected==1 && partitions.length == 0){
                partitions_loading = true; 
                partitions = await get_data(namespace+"."+table, "partitions");  
                partitions_loading = false;  
            }
        } catch (err) {
            error = err.message; 
            partitions_loading = false;  
        }
        try {
            if(selected==2 && snapshots.length == 0){
                snapshots_loading = true;        
                snapshots = await get_data(namespace+"."+table, "snapshots");  
                snapshots_loading = false;              
            }
        } catch (err) {
            error = err.message; 
            snapshots_loading = false;  
        }
        try {
            if(selected==3 && sample_data.length == 0){
                sample_data_loading = true;          
                sample_data = await get_data(namespace+"."+table, "sample");  
                sample_data_loading = false;  
            }
        } catch (err) {
            error = err.message; 
            sample_data_loading = false;  
        }        
        try {
            if(selected==4 && data_change.length == 0){
                data_change_loading = true;          
                data_change = await get_data(namespace+"."+table, "data-change");              
                data_change_loading = false;  
            }
            
        } catch (err) {
            error = err.message; 
            data_change_loading = false;  
        }        
    })();
 
    function set_copy_url(){
        url = window.location.origin;
        url = url+"/?namespace="+namespace+"&table="+table;
    }

    function reset(table){
        partitions = [];
        snapshots = [];
        sample_data = [];
        data_change = [];
        partitions_loading = false;
        snapshots_loading = false;
        sample_data_loading = false;
        data_change_loading = false;
        properties_loading = false;
        partition_specs_loading = false;
        schema_loading = false;
        summary_loading = false;
        schema = [];
        summary = [];
        partition_specs = [];
        properties = [];
        //selected = 0;
    }
   
</script>

<Content>    
    <Tile><h4>Namespace: {namespace} </h4> <p align="right"> <CopyButton text={url} on:click={set_copy_url} iconDescription="Copy table link" feedback="Table link copied" /></p>
        <h4>Table:     {table}</h4>
    </Tile>    
  
    <br />
    <Tabs bind:selected>
        <Tab label="Summary" />
        <Tab label="Partitions" />
        <Tab label="Snapshots" />
        <Tab label="Sample Data" /> 
        <Tab label="Insights" />        
        <svelte:fragment slot="content">
            <TabContent><br/>
                <Grid>
                    <Row>
                      <Column aspectRatio="2x1"> 
                        <br />
                        Summary
                        <br />                        
                        <br />                        
                        <JsonTable jsonData={summary} orient = "kv"></JsonTable>                                                  
                        </Column>
                        <Column aspectRatio="2x1">    
                            <br />                        
                            Schema
                            <br />
                            <br />
                            {#if !schema_loading && schema.length > 0}
                                <VirtualTable data={schema} columns={schema[0]} rowHeight={37} tableHeight={370} defaultColumnWidth={121}/>
                            {/if}
                        </Column>
                    </Row>
                    <Row>
                        <Column aspectRatio="2x1">   
                            <br />                    
                        Properties
                        <br />
                        <br />
                        {#if !properties_loading && properties}
                            <JsonTable jsonData={properties} orient = "kv"></JsonTable>                                                     
                        {/if}
                     </Column>
                      <Column aspectRatio="2x1">  
                        <br />
                        Partition Specs
                        <br />  <br />                        
                        {#if !partition_specs_loading && partition_specs}                        
                            <JsonTable jsonData={partition_specs} orient="table" /> 
                        {/if}
                        </Column>
                    </Row>
                  </Grid>                
            </TabContent>

            <TabContent><br/> 
                {#if partitions_loading}
                    <Loading withOverlay={false} small />      
                {:else if partitions.length > 0}
                    <VirtualTable data={partitions} columns={partitions[0]} rowHeight={35}/>  
                    <br />
                    Total items: {partitions.length}              
                {/if}         
            </TabContent>

            <TabContent><br/>
                {#if snapshots_loading}
                    <Loading withOverlay={false} small />      
                {:else if snapshots.length > 0}
                    <VirtualTable data={snapshots} columns={snapshots[0]} rowHeight={35}/>  
                    <br />
                    Total items: {snapshots.length}     
                {:else}
                    No data         
                {/if}  
            </TabContent>

            <TabContent><br/>
                {#if sample_data_loading}
                    <Loading withOverlay={false} small />      
                {:else if sample_data.length > 0}
                    <VirtualTable data={sample_data} columns={sample_data[0]} rowHeight={35}/>  
                    <br />
                    Sample items: {sample_data.length}              
                {/if}
            </TabContent>

            <TabContent><br/>
                {#if data_change_loading}                    
                    <br /> <Loading withOverlay={false} small /> <br />                    
                {:else}                                    
                    <BarChartSimple data={data_change} options={options} style="padding:2rem;" />     
                {/if}           
            </TabContent>
        </svelte:fragment>
      </Tabs>
    
  </Content>


  <style>
    .table-container {
      height: 500px; /* Set a fixed height for the table */
      overflow: auto; /* Allow scrolling */
      border: 1px solid #ccc;
      border-radius: 5px;
    }
  
   
  
    .header {
      font-weight: bold;
      background-color: #f9f9f9;
      border-bottom: 2px solid #ccc;
      padding: 10px;
    }

    :global(.test1) {
		width: 100%;
		border: 1px solid black;
	}

	:global(.test1 thead) {
		text-align: left;
		border-bottom: 10px solid black;
	}
    :global(.test1 tr) {
		border-bottom: 10px solid grey;
		border-top: 10px solid grey;
	}

	:global(.test1 td:not(:last-of-type)) {
		border-right: 1px solid grey;
	}
	:global(.test1 th:not(:last-of-type)) {
		border-right: 1px solid grey;
	}
    .td:first-of-type,
	td:first-of-type,
	:global(.test1 th:first-of-type) {
		width: 45vw;
	}

	td,
	.td,
	:global(.test1 th) {
		width: calc((45vw - 10px) / 4);
		word-wrap: break-word;
	}

	:global(.test1 th:last-of-type),
	.td:last-of-type,
	td:last-of-type {
		text-align: right;
	}
  </style>
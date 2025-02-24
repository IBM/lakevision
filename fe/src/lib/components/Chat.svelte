<script>
    import { onMount } from 'svelte';
    import { TextInput } from 'carbon-components-svelte';
    import { Button } from 'carbon-components-svelte';
    import Send from "carbon-icons-svelte/lib/Send.svelte";
    import TrashCan from "carbon-icons-svelte/lib/TrashCan.svelte";
    import IbmGranite from "carbon-icons-svelte/lib/IbmGranite.svelte";
    import { Tag } from "carbon-components-svelte";
    import { env } from '$env/dynamic/public';
    export let user;

    let messages = [];
    let message = '';
    let websocket;
    let isConnected = false;

    onMount(() => {
        console.log(user);
        user = user.split('@')[0]
        websocket = new WebSocket(`${env.PUBLIC_CHAT_SERVER}/${user}`);

        websocket.onopen = () => {
            isConnected = true; // Connection is open
            console.log('WebSocket connection opened.');
        };

        websocket.onmessage = (event) => {
            messages = [...messages, event.data];
        };

        websocket.onclose = () => {
            isConnected = false; // Connection is closed
            console.log('WebSocket connection closed.');
        };

        websocket.onerror = (error) => {
            isConnected = false; // Error occurred, consider connection closed
            console.error('WebSocket error:', error);
        };
    });

    function sendMessage() {
        if (websocket && websocket.readyState === WebSocket.OPEN) {
            websocket.send(message);            
            message = '';
        }
        else{
            //websocket = new WebSocket(`ws://9.59.195.229:8000/ws/${user}`); 
        }
    }

    function clearMessages() {
        messages = [];
    }

    function handleKeyDown(event) {
        if (event.key === 'Enter') {
            sendMessage();
        }
    }
</script>
<div class="header">
    <Button on:click={clearMessages} iconDescription="Clear Messages" icon={TrashCan} disabled={messages.length === 0} ></Button>
</div>
<div class="chat-container">
    
    <div class="messages">
        {#each messages as msg}
            <p>{msg}</p>
        {/each}
    </div>    
    <div class="input-area">        
        <TextInput bind:value={message} placeholder="Type your message..." on:keydown={handleKeyDown}/>       
        <Button on:click={sendMessage} iconDescription="Send Message" icon={Send} size="sm"></Button>            
    </div>
    
</div>
<Tag icon={IbmGranite}></Tag>
<style>
    .chat-container {
        width: 100%;
        margin: 20px auto;
        border: 1px solid #ccc;
        padding: 10px;
        display: flex;
        flex-direction: column;
    }

    .messages {
        flex-grow: 1;
        min-height: 200px;
        overflow-y: auto;
        margin-bottom: 10px;
    }

    .input-area {
        display: flex;
    }
    .header {
        display: flex;
        justify-content: flex-end; /* Align to the right */
        margin-bottom: 10px;
    }
</style>
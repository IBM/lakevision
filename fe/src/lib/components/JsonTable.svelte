<script>
    // Accept JSON data as a prop
    export let jsonData = {};
  </script>
  
  <!-- Display the key-value pairs in a table -->
  <table>
    <thead>
      <tr>
        <th></th>
        <th></th>
      </tr>
    </thead>
    <tbody>
      {#each Object.entries(jsonData) as [key, value]}
        <tr>
          <td>{key}</td>
          <td>{value}</td>
        </tr>
      {/each}
    </tbody>
  </table>
  
  <style>
    table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 0px;
    }
  
    th, td {
      border: 1px solid #ccc;
      padding: 8px;
      text-align: left;
    }
  
    th {
      background-color: #f5f5f5;
    }
  </style>
  
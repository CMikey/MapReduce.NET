<%@ Page Title="Home Page" Language="C#" MasterPageFile="~/Site.master" AutoEventWireup="true"
    CodeBehind="Default.aspx.cs" Inherits="MapReduce.Web._Default" %>

<asp:Content ID="HeaderContent" runat="server" ContentPlaceHolderID="HeadContent">
</asp:Content>
<asp:Content ID="BodyContent" runat="server" ContentPlaceHolderID="MainContent">
    <h2>
        Welcome
    </h2>
    <p>
        Start processing and download SQLite result <a href='mapreduce.aspx?Command=getresult&configname=configInvertedIndex.json'>
            [Download]</a>
    </p>
    <p>
        Start processing without download <a href='mapreduce.aspx?Command=start&configname=configInvertedIndex.json'>
            [Start]</a>
    </p>

    
    <p id='status'></p>
    <script type="text/javascript">
        function refreshStatus() {
            $.getJSON('MapReduce.aspx?Command=Status', function (data) {
                var result = $('<div/>').html(JSON.stringify(data));
                $('#status').html(result);
            });
        }
        setInterval(refreshStatus, 1000);
    </script>
    </asp:Content>


using System.Net;

namespace Hangfire.Raft.Tests;

public class RaftStorageOptionsTests
{
    [Fact]
    public void ParseEndpoint_IpLiteral_YieldsIPEndPoint()
    {
        var ip = Assert.IsType<IPEndPoint>(RaftStorageOptions.ParseEndpoint("127.0.0.1:5000"));
        Assert.Equal(IPAddress.Loopback, ip.Address);
        Assert.Equal(5000, ip.Port);
    }

    [Fact]
    public void ParseEndpoint_HostName_YieldsDnsEndPoint_WithoutResolving()
    {
        // The host intentionally does not resolve; parsing must not attempt DNS.
        var dns = Assert.IsType<DnsEndPoint>(RaftStorageOptions.ParseEndpoint("hangfire-0.does-not-exist.invalid:5000"));
        Assert.Equal("hangfire-0.does-not-exist.invalid", dns.Host);
        Assert.Equal(5000, dns.Port);
    }

    [Fact]
    public void ParseEndpoint_BracketedIPv6_YieldsIPEndPoint()
    {
        var ip = Assert.IsType<IPEndPoint>(RaftStorageOptions.ParseEndpoint("[::1]:5000"));
        Assert.Equal(IPAddress.IPv6Loopback, ip.Address);
        Assert.Equal(5000, ip.Port);
    }

    [Theory]
    [InlineData("noport")]
    [InlineData("host:")]
    [InlineData(":5000")]
    [InlineData("::1:5000")] // bare (unbracketed) IPv6 is ambiguous and rejected
    public void ParseEndpoint_Invalid_Throws(string value)
        => Assert.Throws<FormatException>(() => RaftStorageOptions.ParseEndpoint(value));

    [Fact]
    public void RpcEndpoint_PreservesForm_AndOffsetsPort()
    {
        Assert.Equal(
            new IPEndPoint(IPAddress.Loopback, 5001),
            RaftStorageOptions.RpcEndpoint(new IPEndPoint(IPAddress.Loopback, 5000), 1));

        var dns = Assert.IsType<DnsEndPoint>(RaftStorageOptions.RpcEndpoint(new DnsEndPoint("node", 5000), 1));
        Assert.Equal("node", dns.Host);
        Assert.Equal(5001, dns.Port);
    }

    [Theory]
    [InlineData(65000, 1000)] // 66000 overflows the 0-65535 port range
    [InlineData(5000, -6000)] // negative result
    public void RpcEndpoint_Throws_WhenOffsetPushesPortOutOfRange(int raftPort, int offset)
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => RaftStorageOptions.RpcEndpoint(new DnsEndPoint("node", raftPort), offset));
        Assert.Contains("RpcPortOffset", ex.Message); // message names the offending option
    }

    [Fact]
    public void BindAddressFor_UsesLiteralIp_OrAnyForHostNames()
    {
        Assert.Equal(IPAddress.Loopback, RaftStorageOptions.BindAddressFor(new IPEndPoint(IPAddress.Loopback, 5000)));
        Assert.Equal(IPAddress.Any, RaftStorageOptions.BindAddressFor(new DnsEndPoint("node", 5000)));
    }

    [Theory]
    [InlineData("host:99999")]  // out of the 0-65535 range
    [InlineData("host:-1")]
    [InlineData("host:notnum")]
    public void ParseEndpoint_RejectsInvalidPort(string endpoint)
        => Assert.Throws<FormatException>(() => RaftStorageOptions.ParseEndpoint(endpoint));

    [Fact]
    public void ClusterMembers_Throws_WhenEmpty()
    {
        var options = new RaftStorageOptions { SelfEndpoint = "127.0.0.1:5000" };
        Assert.Throws<InvalidOperationException>(() => _ = options.ClusterMembers);
    }

    [Fact]
    public void ClusterMembers_Throws_WhenSelfNotListed()
    {
        var options = new RaftStorageOptions { SelfEndpoint = "127.0.0.1:5000" };
        options.Members.Add("127.0.0.1:6000");
        Assert.Throws<InvalidOperationException>(() => _ = options.ClusterMembers);
    }

    [Fact]
    public void ClusterMembers_Succeeds_WhenSelfListed()
    {
        var options = new RaftStorageOptions { SelfEndpoint = "127.0.0.1:5000" };
        options.Members.Add("127.0.0.1:5000");
        options.Members.Add("node2:5000");
        Assert.Equal(2, options.ClusterMembers.Count);
    }
}

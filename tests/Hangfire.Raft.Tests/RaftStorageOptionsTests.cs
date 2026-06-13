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

    [Fact]
    public void BindAddressFor_UsesLiteralIp_OrAnyForHostNames()
    {
        Assert.Equal(IPAddress.Loopback, RaftStorageOptions.BindAddressFor(new IPEndPoint(IPAddress.Loopback, 5000)));
        Assert.Equal(IPAddress.Any, RaftStorageOptions.BindAddressFor(new DnsEndPoint("node", 5000)));
    }
}

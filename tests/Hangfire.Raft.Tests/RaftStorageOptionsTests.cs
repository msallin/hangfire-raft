using System.Net;

namespace Hangfire.Raft.Tests;

public class RaftStorageOptionsTests
{
    [Test]
    public async Task ParseEndpoint_IpLiteral_YieldsIPEndPoint()
    {
        var endpoint = RaftStorageOptions.ParseEndpoint("127.0.0.1:5000");
        await Assert.That(endpoint).IsTypeOf<IPEndPoint>();
        var ip = (IPEndPoint)endpoint;
        await Assert.That(ip.Address).IsEqualTo(IPAddress.Loopback);
        await Assert.That(ip.Port).IsEqualTo(5000);
    }

    [Test]
    public async Task ParseEndpoint_HostName_YieldsDnsEndPoint_WithoutResolving()
    {
        // The host intentionally does not resolve; parsing must not attempt DNS.
        var endpoint = RaftStorageOptions.ParseEndpoint("hangfire-0.does-not-exist.invalid:5000");
        await Assert.That(endpoint).IsTypeOf<DnsEndPoint>();
        var dns = (DnsEndPoint)endpoint;
        await Assert.That(dns.Host).IsEqualTo("hangfire-0.does-not-exist.invalid");
        await Assert.That(dns.Port).IsEqualTo(5000);
    }

    [Test]
    public async Task ParseEndpoint_BracketedIPv6_YieldsIPEndPoint()
    {
        var endpoint = RaftStorageOptions.ParseEndpoint("[::1]:5000");
        await Assert.That(endpoint).IsTypeOf<IPEndPoint>();
        var ip = (IPEndPoint)endpoint;
        await Assert.That(ip.Address).IsEqualTo(IPAddress.IPv6Loopback);
        await Assert.That(ip.Port).IsEqualTo(5000);
    }

    [Test]
    [Arguments("noport")]
    [Arguments("host:")]
    [Arguments(":5000")]
    [Arguments("::1:5000")] // bare (unbracketed) IPv6 is ambiguous and rejected
    public async Task ParseEndpoint_Invalid_Throws(string value)
        => await Assert.That(() => RaftStorageOptions.ParseEndpoint(value)).ThrowsExactly<FormatException>();

    [Test]
    public async Task RpcEndpoint_PreservesForm_AndOffsetsPort()
    {
        await Assert.That(RaftStorageOptions.RpcEndpoint(new IPEndPoint(IPAddress.Loopback, 5000), 1))
            .IsEqualTo(new IPEndPoint(IPAddress.Loopback, 5001));

        var rpc = RaftStorageOptions.RpcEndpoint(new DnsEndPoint("node", 5000), 1);
        await Assert.That(rpc).IsTypeOf<DnsEndPoint>();
        var dns = (DnsEndPoint)rpc;
        await Assert.That(dns.Host).IsEqualTo("node");
        await Assert.That(dns.Port).IsEqualTo(5001);
    }

    [Test]
    [Arguments(65000, 1000)] // 66000 overflows the 0-65535 port range
    [Arguments(5000, -6000)] // negative result
    public async Task RpcEndpoint_Throws_WhenOffsetPushesPortOutOfRange(int raftPort, int offset)
    {
        // message names the offending option
        await Assert.That(() => RaftStorageOptions.RpcEndpoint(new DnsEndPoint("node", raftPort), offset))
            .ThrowsExactly<InvalidOperationException>()
            .WithMessageContaining("RpcPortOffset");
    }

    [Test]
    public async Task BindAddressFor_UsesLiteralIp_OrAnyForHostNames()
    {
        await Assert.That(RaftStorageOptions.BindAddressFor(new IPEndPoint(IPAddress.Loopback, 5000))).IsEqualTo(IPAddress.Loopback);
        await Assert.That(RaftStorageOptions.BindAddressFor(new DnsEndPoint("node", 5000))).IsEqualTo(IPAddress.Any);
    }

    [Test]
    [Arguments("host:99999")]  // out of the 0-65535 range
    [Arguments("host:-1")]
    [Arguments("host:notnum")]
    public async Task ParseEndpoint_RejectsInvalidPort(string endpoint)
        => await Assert.That(() => RaftStorageOptions.ParseEndpoint(endpoint)).ThrowsExactly<FormatException>();

    [Test]
    public async Task ClusterMembers_Throws_WhenEmpty()
    {
        var options = new RaftStorageOptions { SelfEndpoint = "127.0.0.1:5000" };
        await Assert.That(() => _ = options.ClusterMembers).ThrowsExactly<InvalidOperationException>();
    }

    [Test]
    public async Task ClusterMembers_Throws_WhenSelfNotListed()
    {
        var options = new RaftStorageOptions { SelfEndpoint = "127.0.0.1:5000" };
        options.Members.Add("127.0.0.1:6000");
        await Assert.That(() => _ = options.ClusterMembers).ThrowsExactly<InvalidOperationException>();
    }

    [Test]
    public async Task ClusterMembers_Succeeds_WhenSelfListed()
    {
        var options = new RaftStorageOptions { SelfEndpoint = "127.0.0.1:5000" };
        options.Members.Add("127.0.0.1:5000");
        options.Members.Add("node2:5000");
        await Assert.That(options.ClusterMembers.Count).IsEqualTo(2);
    }

    [Test]
    public async Task ClusterMembers_Deduplicates_RepeatedEntries()
    {
        // A copy-paste duplicate must not inflate the member count: the seeded membership dedupes, and the
        // single-node cold-start fast path keys off this count, so the two must agree.
        var options = new RaftStorageOptions { SelfEndpoint = "127.0.0.1:5000" };
        options.Members.Add("127.0.0.1:5000");
        options.Members.Add("127.0.0.1:5000");
        await Assert.That(options.ClusterMembers.Count).IsEqualTo(1);
    }

    [Test]
    [Arguments("host:0")]   // port 0 binds an ephemeral port but is advertised as :0, so the node is unreachable
    [Arguments("[]:5000")]  // empty bracketed host
    public async Task ParseEndpoint_RejectsUnreachableForms(string endpoint)
        => await Assert.That(() => RaftStorageOptions.ParseEndpoint(endpoint)).ThrowsExactly<FormatException>();

    [Test]
    public async Task Validate_Passes_ForADefaultSingleNodeConfig()
    {
        var options = new RaftStorageOptions { SelfEndpoint = "127.0.0.1:5000" };
        options.Members.Add("127.0.0.1:5000");
        await Assert.That(() => options.Validate()).ThrowsNothing();
    }

    [Test]
    public async Task Validate_Rejects_InvertedElectionTimeouts()
    {
        var options = new RaftStorageOptions { SelfEndpoint = "127.0.0.1:5000", LowerElectionTimeoutMs = 3000, UpperElectionTimeoutMs = 1500 };
        options.Members.Add("127.0.0.1:5000");
        await Assert.That(options.Validate).ThrowsExactly<InvalidOperationException>().WithMessageContaining("ElectionTimeout");
    }

    [Test]
    public async Task Validate_Rejects_NonPositiveSnapshotInterval()
    {
        var options = new RaftStorageOptions { SelfEndpoint = "127.0.0.1:5000", SnapshotInterval = 0 };
        options.Members.Add("127.0.0.1:5000");
        await Assert.That(options.Validate).ThrowsExactly<InvalidOperationException>().WithMessageContaining("SnapshotInterval");
    }

    [Test]
    public async Task Validate_Rejects_NonPositiveLeaseTimeouts()
    {
        var options = new RaftStorageOptions { SelfEndpoint = "127.0.0.1:5000", FetchInvisibilityTimeout = TimeSpan.Zero };
        options.Members.Add("127.0.0.1:5000");
        await Assert.That(options.Validate).ThrowsExactly<InvalidOperationException>().WithMessageContaining("FetchInvisibilityTimeout");
    }

    [Test]
    public async Task Validate_Rejects_RpcPortOffsetThatOverflowsThePortRange()
    {
        var options = new RaftStorageOptions { SelfEndpoint = "127.0.0.1:65000", RpcPortOffset = 1000 };
        options.Members.Add("127.0.0.1:65000");
        await Assert.That(options.Validate).ThrowsExactly<InvalidOperationException>().WithMessageContaining("RpcPortOffset");
    }
}

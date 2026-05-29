# XDP Integration Tests

These tests are run through `cargo xtask xdp-test`.

Local mode runs the tests directly on the host and requires root or equivalent network admin privileges because the harness creates a temporary network namespace, `veth` interfaces, routes, and neighbors:

```bash
cargo xtask xdp-test local --runner "sudo -n -E"
```

To run a single test locally, use this form:

```bash
cargo xtask xdp-test local --runner "sudo -n -E" --test <test-binary> -- <test-name> --exact --nocapture
```

The default suite currently runs:

- `netlink_snapshot`
- `route_monitor`

## Test Topology

Each portable test runs in a fresh temporary network namespace created with `unshare(CLONE_NEWNET)`. The tests bring `lo` up, create the interfaces needed by that test, and restore the original namespace when the test exits.

The initial topology is one veth pair inside that namespace:

```text
temporary test network namespace

  route and neighbor state under test
        |
        v
  axdp0 10.0.0.1/24  02:aa:bb:cc:dd:01
        |
        | veth peer
        |
  axdp1 10.0.0.2/24  02:aa:bb:cc:dd:02

  neighbor: 10.0.0.2 -> 02:aa:bb:cc:dd:02 dev axdp0
  route example: 203.0.113.0/24 via 10.0.0.2 dev axdp0
```

VM mode runs the same Rust test binaries inside a QEMU guest. The host-side xtask builds the test binaries and guest init, builds an initramfs, boots QEMU with the selected kernel, and the guest init runs the tests as PID 1:

```text
host cargo xtask
  |
  | builds test binaries + /init
  | builds initramfs
  v
QEMU guest kernel
  |
  v
/init
  |
  | runs selected test binaries
  v
NetNsGuard inside guest
  |
  v
temporary test network namespace + veth topology
```

```bash
cargo xtask xdp-test fetch-kernels --kernel-set pr
cargo xtask xdp-test vm --kernel-set pr
```

To run a single test in VM mode, use this form:

```bash
cargo xtask xdp-test vm --kernel-set pr --test <test-binary> -- <test-name> --exact --nocapture
```

Requirements for VM mode:

- `qemu-system-x86_64` must be installed and available on `PATH`
- or pass an explicit QEMU path with `--qemu /full/path/to/qemu-system-x86_64`

If QEMU is not installed, `cargo xtask xdp-test vm ...` will fail before booting the guest.

## VM Kernel Sets

The default PR and nightly VM kernel sets are:

- `6.8`
- `6.17`
- `7.0`

The runner downloads missing Debian kernel packages into the XDP VM cache. No manual kernel download is required for the default kernel set.

You can prefetch the default PR kernels without running tests:

```bash
cargo xtask xdp-test fetch-kernels --kernel-set pr
```

You can override the default kernel lists without editing the code. For example, to replace the default PR kernel set for a single run:

```bash
cargo xtask xdp-test fetch-kernels --kernel-set pr --pr-kernel-version 6.6 --pr-kernel-version 6.12
cargo xtask xdp-test vm --kernel-set pr --pr-kernel-version 6.6 --pr-kernel-version 6.12
```

## Individual Tests

Use the local or VM single-test command form above with these test binaries and names:

| Test binary | Test name |
| --- | --- |
| `netlink_snapshot` | `netlink_snapshot_reads_the_prepared_namespace` |
| `route_monitor` | `route_monitor_publishes_live_route_updates` |

## Test Coverage

`netlink_snapshot`:

- `netlink_snapshot_reads_the_prepared_namespace`: reads interfaces, routes, and neighbors from the temporary namespace and verifies the prepared veth route and permanent neighbor are visible through netlink.

`route_monitor`:

- `route_monitor_publishes_live_route_updates`: verifies the route monitor publishes an added route with the expected next hop and later removes it after the route is deleted.

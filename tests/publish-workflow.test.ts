import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const workflow = readFileSync(new URL("../.github/workflows/publish.yml", import.meta.url), "utf8");
const ordinary = workflow.slice(workflow.indexOf("  publish:\n"),
  workflow.indexOf("  publish-cloud-runtime-test:\n"));
const isolated = workflow.slice(workflow.indexOf("  publish-cloud-runtime-test:\n"),
  workflow.indexOf("  diagnose-cloud-runtime-test-access:\n"));
const diagnostic = workflow.slice(workflow.indexOf("  diagnose-cloud-runtime-test-access:\n"));

describe("isolated Cloud runtime SDK release", () => {
  it("leaves the normal main/latest path inactive during test-only dispatch", () => {
    expect(ordinary).toContain("if: ${{ github.ref == 'refs/heads/main' && (github.event_name != 'workflow_dispatch' || inputs.test_only != true) }}");
    expect(ordinary).not.toContain("refs/heads/feat/cloud-runtime-message-transport");
    expect(ordinary).toContain("run: npm publish --access public");
    expect(ordinary).not.toContain("cloud-runtime-test");
    expect(isolated).toContain("if: ${{ github.event_name == 'workflow_dispatch' && inputs.test_only == true && inputs.diagnostic_only != true }}");
  });

  it("pins the reviewed feature SHA and exact one-off version before npm publish", () => {
    expect(isolated).toContain("CLOUD_TEST_VERSION: 2.0.17-cloud.0");
    expect(isolated).toContain("test \"$DISPATCH_REF\" = refs/heads/feat/cloud-runtime-message-transport");
    expect(isolated).toContain('test "$DISPATCH_SHA" = "$EXPECTED_SHA"');
    expect(isolated).toContain('test "$(git rev-parse HEAD)" = "$EXPECTED_SHA"');
    expect(isolated).toContain('test "$(node -p \'require("./package.json").version\')" = "$CLOUD_TEST_VERSION"');
    expect(isolated).toContain("npm run build");
    expect(isolated).toContain("npm test");
    expect(isolated).toContain("--ignore-scripts --access public --tag cloud-runtime-test");
    expect(isolated).not.toContain("--tag latest");
  });

  it("confirms published source/integrity and preserves standard tags", () => {
    expect(isolated).toContain("for attempt in {1..12}; do");
    expect(isolated).toContain("grep -Eq 'E404|404 Not Found'");
    expect(isolated).toContain("sleep 10");
    expect(isolated).not.toContain("npm publish --tag latest");
    expect(isolated).toContain('test "$(git rev-parse HEAD)" = "$EXPECTED_SHA"');
    expect(isolated).toContain('sourceSha:$sourceSha,version:$version,runId:$runId,runAttempt:$runAttempt,integrity:$integrity');
    expect(isolated).toContain("test \"$(jq -er '.\"dist.integrity\"' \"$RUNNER_TEMP/published.json\")\"");
    expect(isolated).toContain('"cloud-runtime-test"');
    expect(isolated).toContain("for tag in latest beta staging production-candidate; do");
    expect(isolated).toContain("cloud-runtime-test-receipt.json");
    expect(isolated).toContain("if: always()");
    expect(isolated).not.toContain("gitHead");
  });

  it("keeps npm access diagnosis read-only and separate from publication", () => {
    expect(diagnostic).toContain("if: ${{ github.event_name == 'workflow_dispatch' && inputs.test_only == true && inputs.diagnostic_only == true }}");
    expect(diagnostic).toContain('test "$DISPATCH_SHA" = "$EXPECTED_SHA"');
    expect(diagnostic).toContain('npm whoami --registry https://registry.npmjs.org');
    expect(diagnostic).toContain('npm access list packages "$actor" @beeos-ai/message-sdk --json');
    expect(diagnostic).toContain('npm_package_read_write=true');
    expect(diagnostic).not.toContain("npm publish");
    expect(diagnostic).not.toContain("npm access grant");
  });
});

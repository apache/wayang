<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->
# Wayang Security

The Apache Software Foundation (ASF) and the Apache Wayang community take security very seriously. 
Apache Wayang specifically provides robust security features and actively addresses concerns around potential vulnerabilities. 
If you have discovered a vulnerability or have concerns regarding Apache Wayang security, please immediately contact the security team via email at 
[security@wayang.apache.org](mailto:security@wayang.apache.org).

In your email, please include:
- A detailed description of the security issue
- Steps to reproduce the vulnerability, if possible

Upon receiving your report, our security team will review the provided information and respond accordingly.

Please reserve the security address exclusively for reporting undisclosed vulnerabilities. For general security-related questions, usage of security features, or addressing known fixed issues, please utilize our user and developer mailing lists instead. Do not publicly disclose vulnerabilities without first reporting them to the Apache Wayang security team.

The ASF Security team maintains detailed guidelines on managing and addressing vulnerabilities. For further information, please refer to the [ASF Security Page](https://www.apache.org/security/).

## Advisories for Dependencies

Many organizations employ security scanning tools to identify components with known security advisories. Although we strongly recommend these tools as they can alert users to potential risks, they often generate false positives. This occurs because a vulnerable dependency may not necessarily impact Apache Wayang if used in a non-exploitable manner.

Therefore, advisories regarding Apache Wayang's dependencies are not automatically considered critical. However, if additional analysis indicates that Apache Wayang might be affected by a dependency's vulnerability, please report your findings privately to [security@wayang.apache.org](mailto:security@wayang.apache.org).

If a dependency advisory is identified, please:

1. Verify if our DependencyCheck suppressions contain relevant details.
2. Check our issue tracker for discussions regarding this advisory.
3. Conduct your own analysis to determine whether Apache Wayang is affected.
   - If affected, report your findings privately through [security@wayang.apache.org](mailto:security@wayang.apache.org).
   - If not affected, please contribute by updating the DependencyCheck suppression list, clearly documenting why Apache Wayang is not impacted.

## Component-Specific Security Notes

### Wayang JSON REST API

The `wayang-api-json` module exposes a REST endpoint that accepts a WayangPlan in JSON format, including UDFs to be executed by the targeted engine. **This endpoint has no built-in authentication or authorization** — any client that can reach it can submit UDFs that will execute with the privileges of the Wayang process.

This is a deliberate scope decision: the module is intended for use on a trusted network only (localhost, a private subnet, or behind a VPN). 
**If the endpoint is exposed on a public IP, an authentication and encryption layer is recommended**.

Questions on this are welcome at [security@wayang.apache.org](mailto:security@wayang.apache.org).

The platform frontend is built into dist/ by npm run build from platform/.
This file lets Go compile and run gateway tests before a frontend build exists.
The platform command reports an actionable error if its embedded build is missing.

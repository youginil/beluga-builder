export function respondError(ctx, status, reason) {
  ctx.response.status = status;
  ctx.response.body = reason;
}

export function respondJSON(ctx, data = {}) {
  ctx.response.status = 200;
  ctx.response.type = "application/json";
  ctx.response.body = data;
}

export function respondBuffer(ctx, data) {
  ctx.response.status = 200;
  ctx.response.body = data;
}

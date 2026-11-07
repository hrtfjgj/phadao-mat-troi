(() => {
  const rt = localStorage.getItem('refresh_token');
  if (!rt) {
    console.log('[GRAB] Khong thay refresh_token. Dang nhap web Sunwin roi chay lai.');
    return;
  }
  console.log('%c[REFRESH_TOKEN] Copy gia tri duoi vao bien moi truong REFRESH_TOKEN (Render) hoac config.json:', 'color:#0f0;font-weight:bold');
  console.log(rt);
  return rt;
})();
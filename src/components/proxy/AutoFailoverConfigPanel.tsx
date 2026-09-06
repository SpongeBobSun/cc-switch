import { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Alert, AlertDescription } from "@/components/ui/alert";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { Switch } from "@/components/ui/switch";
import { Save, Loader2, Info, ChevronDown } from "lucide-react";
import { toast } from "sonner";
import { useAppProxyConfig, useUpdateAppProxyConfig } from "@/lib/query/proxy";

export interface AutoFailoverConfigPanelProps {
  appType: string;
  disabled?: boolean;
}

export function AutoFailoverConfigPanel({
  appType,
  disabled = false,
}: AutoFailoverConfigPanelProps) {
  const { t } = useTranslation();
  const { data: config, isLoading, error } = useAppProxyConfig(appType);
  const updateConfig = useUpdateAppProxyConfig();
  const [rateLimitSettingsOpen, setRateLimitSettingsOpen] = useState(false);

  // 使用字符串状态以支持完全清空数字输入框
  const [formData, setFormData] = useState({
    autoFailoverEnabled: false,
    maxRetries: "3",
    streamingFirstByteTimeout: "60",
    streamingIdleTimeout: "120",
    nonStreamingTimeout: "600",
    circuitFailureThreshold: "5",
    circuitSuccessThreshold: "2",
    circuitTimeoutSeconds: "60",
    circuitErrorRateThreshold: "50", // 存储百分比值
    circuitMinRequests: "10",
    retryOnRateLimit: true,
    rateLimitMaxRetries: "3",
    rateLimitMaxWaitSeconds: "60",
    rateLimitTotalWaitSeconds: "60",
    rateLimitRespectRetryAfter: true,
  });

  useEffect(() => {
    if (config) {
      setFormData({
        autoFailoverEnabled: config.autoFailoverEnabled,
        maxRetries: String(config.maxRetries),
        streamingFirstByteTimeout: String(config.streamingFirstByteTimeout),
        streamingIdleTimeout: String(config.streamingIdleTimeout),
        nonStreamingTimeout: String(config.nonStreamingTimeout),
        circuitFailureThreshold: String(config.circuitFailureThreshold),
        circuitSuccessThreshold: String(config.circuitSuccessThreshold),
        circuitTimeoutSeconds: String(config.circuitTimeoutSeconds),
        circuitErrorRateThreshold: String(
          Math.round(config.circuitErrorRateThreshold * 100),
        ),
        circuitMinRequests: String(config.circuitMinRequests),
        retryOnRateLimit: config.retryOnRateLimit,
        rateLimitMaxRetries: String(config.rateLimitMaxRetries),
        rateLimitMaxWaitSeconds: String(config.rateLimitMaxWaitSeconds),
        rateLimitTotalWaitSeconds: String(config.rateLimitTotalWaitSeconds),
        rateLimitRespectRetryAfter: config.rateLimitRespectRetryAfter,
      });
    }
  }, [config]);

  const handleSave = async () => {
    if (!config) return;
    // 解析数字，返回 NaN 表示无效输入
    const parseNum = (val: string) => {
      const trimmed = val.trim();
      // 必须是纯数字
      if (!/^-?\d+$/.test(trimmed)) return NaN;
      return parseInt(trimmed);
    };

    // 定义各字段的有效范围
    const ranges = {
      maxRetries: { min: 0, max: 10 },
      streamingFirstByteTimeout: { min: 1, max: 120 },
      streamingIdleTimeout: { min: 0, max: 600 },
      nonStreamingTimeout: { min: 60, max: 1200 },
      circuitFailureThreshold: { min: 1, max: 20 },
      circuitSuccessThreshold: { min: 1, max: 10 },
      circuitTimeoutSeconds: { min: 0, max: 300 },
      circuitErrorRateThreshold: { min: 0, max: 100 },
      circuitMinRequests: { min: 5, max: 100 },
      rateLimitMaxRetries: { min: 0, max: 5 },
      rateLimitMaxWaitSeconds: { min: 1, max: 300 },
      rateLimitTotalWaitSeconds: { min: 1, max: 600 },
    };

    // 解析原始值
    const raw = {
      maxRetries: parseNum(formData.maxRetries),
      streamingFirstByteTimeout: parseNum(formData.streamingFirstByteTimeout),
      streamingIdleTimeout: parseNum(formData.streamingIdleTimeout),
      nonStreamingTimeout: parseNum(formData.nonStreamingTimeout),
      circuitFailureThreshold: parseNum(formData.circuitFailureThreshold),
      circuitSuccessThreshold: parseNum(formData.circuitSuccessThreshold),
      circuitTimeoutSeconds: parseNum(formData.circuitTimeoutSeconds),
      circuitErrorRateThreshold: parseNum(formData.circuitErrorRateThreshold),
      circuitMinRequests: parseNum(formData.circuitMinRequests),
      rateLimitMaxRetries: parseNum(formData.rateLimitMaxRetries),
      rateLimitMaxWaitSeconds: parseNum(formData.rateLimitMaxWaitSeconds),
      rateLimitTotalWaitSeconds: parseNum(formData.rateLimitTotalWaitSeconds),
    };

    // 校验是否超出范围（NaN 也视为无效）
    const errors: string[] = [];
    const checkRange = (
      value: number,
      range: { min: number; max: number },
      label: string,
    ) => {
      if (isNaN(value) || value < range.min || value > range.max) {
        errors.push(`${label}: ${range.min}-${range.max}`);
      }
    };

    checkRange(
      raw.maxRetries,
      ranges.maxRetries,
      t("proxy.autoFailover.maxRetries", "最大重试次数"),
    );
    checkRange(
      raw.streamingFirstByteTimeout,
      ranges.streamingFirstByteTimeout,
      t("proxy.autoFailover.streamingFirstByte", "流式首字节超时"),
    );
    checkRange(
      raw.streamingIdleTimeout,
      ranges.streamingIdleTimeout,
      t("proxy.autoFailover.streamingIdle", "流式静默超时"),
    );
    checkRange(
      raw.nonStreamingTimeout,
      ranges.nonStreamingTimeout,
      t("proxy.autoFailover.nonStreaming", "非流式超时"),
    );
    checkRange(
      raw.circuitFailureThreshold,
      ranges.circuitFailureThreshold,
      t("proxy.autoFailover.failureThreshold", "失败阈值"),
    );
    checkRange(
      raw.circuitSuccessThreshold,
      ranges.circuitSuccessThreshold,
      t("proxy.autoFailover.successThreshold", "恢复成功阈值"),
    );
    checkRange(
      raw.circuitTimeoutSeconds,
      ranges.circuitTimeoutSeconds,
      t("proxy.autoFailover.timeout", "恢复等待时间"),
    );
    checkRange(
      raw.circuitErrorRateThreshold,
      ranges.circuitErrorRateThreshold,
      t("proxy.autoFailover.errorRate", "错误率阈值"),
    );
    checkRange(
      raw.circuitMinRequests,
      ranges.circuitMinRequests,
      t("proxy.autoFailover.minRequests", "最小请求数"),
    );
    if (formData.retryOnRateLimit) {
      checkRange(
        raw.rateLimitMaxRetries,
        ranges.rateLimitMaxRetries,
        t("proxy.autoFailover.rateLimitMaxRetries", "429 同站点最大重试次数"),
      );
      checkRange(
        raw.rateLimitMaxWaitSeconds,
        ranges.rateLimitMaxWaitSeconds,
        t("proxy.autoFailover.rateLimitMaxWait", "429 最大等待时间"),
      );
      checkRange(
        raw.rateLimitTotalWaitSeconds,
        ranges.rateLimitTotalWaitSeconds,
        t("proxy.autoFailover.rateLimitTotalWait", "429 累计等待上限"),
      );
    }

    if (errors.length > 0) {
      toast.error(
        t("proxy.autoFailover.validationFailed", {
          fields: errors.join("; "),
          defaultValue: `以下字段超出有效范围: ${errors.join("; ")}`,
        }),
      );
      return;
    }

    try {
      await updateConfig.mutateAsync({
        appType,
        enabled: config.enabled,
        autoFailoverEnabled: formData.autoFailoverEnabled,
        maxRetries: raw.maxRetries,
        streamingFirstByteTimeout: raw.streamingFirstByteTimeout,
        streamingIdleTimeout: raw.streamingIdleTimeout,
        nonStreamingTimeout: raw.nonStreamingTimeout,
        circuitFailureThreshold: raw.circuitFailureThreshold,
        circuitSuccessThreshold: raw.circuitSuccessThreshold,
        circuitTimeoutSeconds: raw.circuitTimeoutSeconds,
        circuitErrorRateThreshold: raw.circuitErrorRateThreshold / 100,
        circuitMinRequests: raw.circuitMinRequests,
        retryOnRateLimit: formData.retryOnRateLimit,
        rateLimitMaxRetries: raw.rateLimitMaxRetries,
        rateLimitMaxWaitSeconds: raw.rateLimitMaxWaitSeconds,
        rateLimitTotalWaitSeconds: raw.rateLimitTotalWaitSeconds,
        rateLimitRespectRetryAfter: formData.rateLimitRespectRetryAfter,
      });
      toast.success(
        t("proxy.autoFailover.configSaved", "自动故障转移配置已保存"),
        { closeButton: true },
      );
    } catch (e) {
      toast.error(
        t("proxy.autoFailover.configSaveFailed", "保存失败") + ": " + String(e),
      );
    }
  };

  const handleReset = () => {
    if (config) {
      setFormData({
        autoFailoverEnabled: config.autoFailoverEnabled,
        maxRetries: String(config.maxRetries),
        streamingFirstByteTimeout: String(config.streamingFirstByteTimeout),
        streamingIdleTimeout: String(config.streamingIdleTimeout),
        nonStreamingTimeout: String(config.nonStreamingTimeout),
        circuitFailureThreshold: String(config.circuitFailureThreshold),
        circuitSuccessThreshold: String(config.circuitSuccessThreshold),
        circuitTimeoutSeconds: String(config.circuitTimeoutSeconds),
        circuitErrorRateThreshold: String(
          Math.round(config.circuitErrorRateThreshold * 100),
        ),
        circuitMinRequests: String(config.circuitMinRequests),
        retryOnRateLimit: config.retryOnRateLimit,
        rateLimitMaxRetries: String(config.rateLimitMaxRetries),
        rateLimitMaxWaitSeconds: String(config.rateLimitMaxWaitSeconds),
        rateLimitTotalWaitSeconds: String(config.rateLimitTotalWaitSeconds),
        rateLimitRespectRetryAfter: config.rateLimitRespectRetryAfter,
      });
    }
  };

  if (isLoading) {
    return (
      <div className="flex items-center justify-center p-4">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    );
  }

  const isDisabled = disabled || updateConfig.isPending;
  const rateLimitInputsDisabled = isDisabled || !formData.retryOnRateLimit;

  return (
    <div className="border-0 rounded-none shadow-none bg-transparent">
      <div className="space-y-4">
        {error && (
          <Alert variant="destructive">
            <AlertDescription>{String(error)}</AlertDescription>
          </Alert>
        )}

        <Alert className="border-blue-500/40 bg-blue-500/10">
          <Info className="h-4 w-4" />
          <AlertDescription className="text-sm">
            {t(
              "proxy.autoFailover.info",
              "当故障转移队列中配置了多个供应商时，系统会在请求失败时按优先级顺序依次尝试。当某个供应商连续失败达到阈值时，熔断器会打开并在一段时间内跳过该供应商。",
            )}
          </AlertDescription>
        </Alert>

        <Collapsible
          open={rateLimitSettingsOpen}
          onOpenChange={setRateLimitSettingsOpen}
        >
          <CollapsibleTrigger asChild>
            <button
              type="button"
              className="flex w-full items-center justify-between gap-3 rounded-lg border border-white/10 bg-muted/30 px-4 py-3 text-left transition-colors hover:bg-muted/50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            >
              <span>
                <span className="block text-sm font-semibold">
                  {t("proxy.autoFailover.rateLimitTitle", "429 限流自动重试")}
                </span>
                <span className="block pt-1 text-xs text-muted-foreground">
                  {t(
                    "proxy.autoFailover.rateLimitDescription",
                    "先等待并重试当前供应商，失败后再继续故障转移",
                  )}
                </span>
              </span>
              <ChevronDown
                className={`h-4 w-4 shrink-0 text-muted-foreground transition-transform ${
                  rateLimitSettingsOpen ? "rotate-180" : ""
                }`}
                aria-hidden="true"
              />
            </button>
          </CollapsibleTrigger>
          <CollapsibleContent>
            <div className="mt-3 space-y-4 rounded-lg border border-white/10 bg-muted/30 p-4">
              <div className="flex items-center justify-between gap-4">
                <div className="space-y-1">
                  <Label htmlFor={`retryOnRateLimit-${appType}`}>
                    {t(
                      "proxy.autoFailover.rateLimitEnabled",
                      "启用 429 自动重试",
                    )}
                  </Label>
                  <p className="text-xs text-muted-foreground">
                    {t(
                      "proxy.autoFailover.rateLimitEnabledHint",
                      "仅在自动故障转移启用时生效",
                    )}
                  </p>
                </div>
                <Switch
                  id={`retryOnRateLimit-${appType}`}
                  checked={formData.retryOnRateLimit}
                  onCheckedChange={(checked) =>
                    setFormData({ ...formData, retryOnRateLimit: checked })
                  }
                  disabled={isDisabled}
                />
              </div>

              <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
                <div className="space-y-2">
                  <Label htmlFor={`rateLimitMaxRetries-${appType}`}>
                    {t(
                      "proxy.autoFailover.rateLimitMaxRetries",
                      "同站点最大重试次数",
                    )}
                  </Label>
                  <Input
                    id={`rateLimitMaxRetries-${appType}`}
                    type="number"
                    min="0"
                    max="5"
                    value={formData.rateLimitMaxRetries}
                    onChange={(event) =>
                      setFormData({
                        ...formData,
                        rateLimitMaxRetries: event.target.value,
                      })
                    }
                    disabled={rateLimitInputsDisabled}
                  />
                  <p className="text-xs text-muted-foreground">
                    {t(
                      "proxy.autoFailover.rateLimitMaxRetriesHint",
                      "429 后在当前供应商重发请求的次数（0-5，默认 3 次）",
                    )}
                  </p>
                </div>

                <div className="space-y-2">
                  <Label htmlFor={`rateLimitMaxWait-${appType}`}>
                    {t(
                      "proxy.autoFailover.rateLimitMaxWait",
                      "单次最大等待时间（秒）",
                    )}
                  </Label>
                  <Input
                    id={`rateLimitMaxWait-${appType}`}
                    type="number"
                    min="1"
                    max="300"
                    value={formData.rateLimitMaxWaitSeconds}
                    onChange={(event) =>
                      setFormData({
                        ...formData,
                        rateLimitMaxWaitSeconds: event.target.value,
                      })
                    }
                    disabled={rateLimitInputsDisabled}
                  />
                  <p className="text-xs text-muted-foreground">
                    {t(
                      "proxy.autoFailover.rateLimitMaxWaitHint",
                      "上游要求等待更久时，立即改走下一个供应商（1-300 秒，默认 60 秒）",
                    )}
                  </p>
                </div>

                <div className="space-y-2">
                  <Label htmlFor={`rateLimitTotalWait-${appType}`}>
                    {t(
                      "proxy.autoFailover.rateLimitTotalWait",
                      "本次请求累计等待上限（秒）",
                    )}
                  </Label>
                  <Input
                    id={`rateLimitTotalWait-${appType}`}
                    type="number"
                    min="1"
                    max="600"
                    value={formData.rateLimitTotalWaitSeconds}
                    onChange={(event) =>
                      setFormData({
                        ...formData,
                        rateLimitTotalWaitSeconds: event.target.value,
                      })
                    }
                    disabled={rateLimitInputsDisabled}
                  />
                  <p className="text-xs text-muted-foreground">
                    {t(
                      "proxy.autoFailover.rateLimitTotalWaitHint",
                      "所有容灾供应商共享此等待预算，耗尽后继续故障转移（1-600 秒，默认 60 秒）",
                    )}
                  </p>
                </div>
              </div>

              <div className="flex items-center justify-between gap-4 rounded-md border border-border/50 p-3">
                <div className="space-y-1">
                  <Label htmlFor={`respectRetryAfter-${appType}`}>
                    {t(
                      "proxy.autoFailover.respectRetryAfter",
                      "优先遵守 Retry-After",
                    )}
                  </Label>
                  <p className="text-xs text-muted-foreground">
                    {t(
                      "proxy.autoFailover.respectRetryAfterHint",
                      "未提供或格式无效时使用约 2 秒、4 秒、8 秒的带抖动退避等待",
                    )}
                  </p>
                </div>
                <Switch
                  id={`respectRetryAfter-${appType}`}
                  checked={formData.rateLimitRespectRetryAfter}
                  onCheckedChange={(checked) =>
                    setFormData({
                      ...formData,
                      rateLimitRespectRetryAfter: checked,
                    })
                  }
                  disabled={rateLimitInputsDisabled}
                />
              </div>

              <p className="text-xs text-amber-600 dark:text-amber-400">
                {t(
                  "proxy.autoFailover.rateLimitRetryWarning",
                  "重试会重发原始请求；通常 429 表示上游未接受请求，但异常上游仍可能出现重复执行。",
                )}
              </p>
            </div>
          </CollapsibleContent>
        </Collapsible>

        {/* 重试与超时配置 */}
        <div className="space-y-4 rounded-lg border border-white/10 bg-muted/30 p-4">
          <h4 className="text-sm font-semibold">
            {t("proxy.autoFailover.retrySettings", "重试与超时设置")}
          </h4>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label htmlFor={`maxRetries-${appType}`}>
                {t("proxy.autoFailover.maxRetries", "最大重试次数")}
              </Label>
              <Input
                id={`maxRetries-${appType}`}
                type="number"
                min="0"
                max="10"
                value={formData.maxRetries}
                onChange={(e) =>
                  setFormData({ ...formData, maxRetries: e.target.value })
                }
                disabled={isDisabled}
              />
              <p className="text-xs text-muted-foreground">
                {t(
                  "proxy.autoFailover.maxRetriesHint",
                  "请求失败时的重试次数（0-10）",
                )}
              </p>
            </div>

            <div className="space-y-2">
              <Label htmlFor={`failureThreshold-${appType}`}>
                {t("proxy.autoFailover.failureThreshold", "失败阈值")}
              </Label>
              <Input
                id={`failureThreshold-${appType}`}
                type="number"
                min="1"
                max="20"
                value={formData.circuitFailureThreshold}
                onChange={(e) =>
                  setFormData({
                    ...formData,
                    circuitFailureThreshold: e.target.value,
                  })
                }
                disabled={isDisabled}
              />
              <p className="text-xs text-muted-foreground">
                {t(
                  "proxy.autoFailover.failureThresholdHint",
                  "连续失败多少次后打开熔断器（建议: 3-10）",
                )}
              </p>
            </div>
          </div>
        </div>

        {/* 超时配置 */}
        <div className="space-y-4 rounded-lg border border-white/10 bg-muted/30 p-4">
          <h4 className="text-sm font-semibold">
            {t("proxy.autoFailover.timeoutSettings", "超时配置")}
          </h4>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="space-y-2">
              <Label htmlFor={`streamingFirstByte-${appType}`}>
                {t(
                  "proxy.autoFailover.streamingFirstByte",
                  "流式首字节超时（秒）",
                )}
              </Label>
              <Input
                id={`streamingFirstByte-${appType}`}
                type="number"
                min="1"
                max="120"
                value={formData.streamingFirstByteTimeout}
                onChange={(e) =>
                  setFormData({
                    ...formData,
                    streamingFirstByteTimeout: e.target.value,
                  })
                }
                disabled={isDisabled}
              />
              <p className="text-xs text-muted-foreground">
                {t(
                  "proxy.autoFailover.streamingFirstByteHint",
                  "等待首个数据块的最大时间，范围 1-120 秒，默认 60 秒",
                )}
              </p>
            </div>

            <div className="space-y-2">
              <Label htmlFor={`streamingIdle-${appType}`}>
                {t("proxy.autoFailover.streamingIdle", "流式静默超时（秒）")}
              </Label>
              <Input
                id={`streamingIdle-${appType}`}
                type="number"
                min="0"
                max="600"
                value={formData.streamingIdleTimeout}
                onChange={(e) =>
                  setFormData({
                    ...formData,
                    streamingIdleTimeout: e.target.value,
                  })
                }
                disabled={isDisabled}
              />
              <p className="text-xs text-muted-foreground">
                {t(
                  "proxy.autoFailover.streamingIdleHint",
                  "数据块之间的最大间隔，范围 60-600 秒，填 0 禁用（防止中途卡住）",
                )}
              </p>
            </div>

            <div className="space-y-2">
              <Label htmlFor={`nonStreaming-${appType}`}>
                {t("proxy.autoFailover.nonStreaming", "非流式超时（秒）")}
              </Label>
              <Input
                id={`nonStreaming-${appType}`}
                type="number"
                min="60"
                max="1200"
                value={formData.nonStreamingTimeout}
                onChange={(e) =>
                  setFormData({
                    ...formData,
                    nonStreamingTimeout: e.target.value,
                  })
                }
                disabled={isDisabled}
              />
              <p className="text-xs text-muted-foreground">
                {t(
                  "proxy.autoFailover.nonStreamingHint",
                  "非流式请求的总超时时间，范围 60-1200 秒，默认 600 秒（10 分钟）",
                )}
              </p>
            </div>
          </div>
        </div>

        {/* 熔断器配置 */}
        <div className="space-y-4 rounded-lg border border-white/10 bg-muted/30 p-4">
          <h4 className="text-sm font-semibold">
            {t("proxy.autoFailover.circuitBreakerSettings", "熔断器配置")}
          </h4>

          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div className="space-y-2">
              <Label htmlFor={`successThreshold-${appType}`}>
                {t("proxy.autoFailover.successThreshold", "恢复成功阈值")}
              </Label>
              <Input
                id={`successThreshold-${appType}`}
                type="number"
                min="1"
                max="10"
                value={formData.circuitSuccessThreshold}
                onChange={(e) =>
                  setFormData({
                    ...formData,
                    circuitSuccessThreshold: e.target.value,
                  })
                }
                disabled={isDisabled}
              />
              <p className="text-xs text-muted-foreground">
                {t(
                  "proxy.autoFailover.successThresholdHint",
                  "半开状态下成功多少次后关闭熔断器",
                )}
              </p>
            </div>

            <div className="space-y-2">
              <Label htmlFor={`timeoutSeconds-${appType}`}>
                {t("proxy.autoFailover.timeout", "恢复等待时间（秒）")}
              </Label>
              <Input
                id={`timeoutSeconds-${appType}`}
                type="number"
                min="0"
                max="300"
                value={formData.circuitTimeoutSeconds}
                onChange={(e) =>
                  setFormData({
                    ...formData,
                    circuitTimeoutSeconds: e.target.value,
                  })
                }
                disabled={isDisabled}
              />
              <p className="text-xs text-muted-foreground">
                {t(
                  "proxy.autoFailover.timeoutHint",
                  "熔断器打开后，等待多久后尝试恢复（建议: 30-120）",
                )}
              </p>
            </div>

            <div className="space-y-2">
              <Label htmlFor={`errorRateThreshold-${appType}`}>
                {t("proxy.autoFailover.errorRate", "错误率阈值 (%)")}
              </Label>
              <Input
                id={`errorRateThreshold-${appType}`}
                type="number"
                min="0"
                max="100"
                step="5"
                value={formData.circuitErrorRateThreshold}
                onChange={(e) =>
                  setFormData({
                    ...formData,
                    circuitErrorRateThreshold: e.target.value,
                  })
                }
                disabled={isDisabled}
              />
              <p className="text-xs text-muted-foreground">
                {t(
                  "proxy.autoFailover.errorRateHint",
                  "错误率超过此值时打开熔断器",
                )}
              </p>
            </div>

            <div className="space-y-2">
              <Label htmlFor={`minRequests-${appType}`}>
                {t("proxy.autoFailover.minRequests", "最小请求数")}
              </Label>
              <Input
                id={`minRequests-${appType}`}
                type="number"
                min="5"
                max="100"
                value={formData.circuitMinRequests}
                onChange={(e) =>
                  setFormData({
                    ...formData,
                    circuitMinRequests: e.target.value,
                  })
                }
                disabled={isDisabled}
              />
              <p className="text-xs text-muted-foreground">
                {t(
                  "proxy.autoFailover.minRequestsHint",
                  "计算错误率前的最小请求数",
                )}
              </p>
            </div>
          </div>
        </div>

        {/* 操作按钮 */}
        <div className="flex justify-end gap-3 pt-2">
          <Button variant="outline" onClick={handleReset} disabled={isDisabled}>
            {t("common.reset", "重置")}
          </Button>
          <Button onClick={handleSave} disabled={isDisabled}>
            {updateConfig.isPending ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                {t("common.saving", "保存中...")}
              </>
            ) : (
              <>
                <Save className="mr-2 h-4 w-4" />
                {t("common.save", "保存")}
              </>
            )}
          </Button>
        </div>
      </div>
    </div>
  );
}

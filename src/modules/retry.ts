import { DelayFunction } from '../types';

const MAX_DELAY_MS = 5000;

/**
 * Internal use of Subscription link
 * @private
 */
export class NonRetryableError extends Error {
  public readonly nonRetryable = true;
  constructor(message: string) {
    super(message);
  }
}

const isNonRetryableError = (obj: any): obj is NonRetryableError => {
  const key: keyof NonRetryableError = "nonRetryable";
  return obj && obj[key];
};

/**
 * @private
 * Internal use of Subscription link
 */
export const retry = async (
  functionToRetry: Function,
  args: any[],
  delayFn: DelayFunction,
  attempt = 1
): Promise<any> => {
  try {
    await functionToRetry(...args);
  } catch (err) {
    console.log(`error ${err}`);
    if (isNonRetryableError(err)) {
      console.log("non retryable error");
      throw err;
    }

    const retryIn = delayFn(attempt, args, err);
    console.log("retryIn ", retryIn);
    if (retryIn !== false) {
      await new Promise(res => setTimeout(res, retryIn));
      return await retry(functionToRetry, args, delayFn, attempt + 1);
    } else {
      throw err;
    }
  }
}

const jitteredBackoff = (maxDelayMs: number): DelayFunction => {
  const BASE_TIME_MS = 100;
  const JITTER_FACTOR = 100;

  return attempt => {
    const delay = 2 ** attempt * BASE_TIME_MS + JITTER_FACTOR * Math.random();
    return delay > maxDelayMs ? false : delay;
  };
}

/**
 * @private
 * Internal use of Subscription link
 */
export const jitteredExponentialRetry = (
  functionToRetry: Function,
  args: any[],
  maxDelayMs: number = MAX_DELAY_MS
) => retry(functionToRetry, args, jitteredBackoff(maxDelayMs));

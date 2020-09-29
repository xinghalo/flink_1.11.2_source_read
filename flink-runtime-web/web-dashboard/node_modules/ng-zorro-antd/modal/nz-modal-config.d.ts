import { InjectionToken } from '@angular/core';
export declare const NZ_MODAL_DEFAULT_CONFIG: NzModalConfig;
export declare const NZ_MODAL_CONFIG: InjectionToken<NzModalConfig>;
export interface NzModalConfig {
    /**
     * @deprecated used {@link BlockScrollStrategy} instead.
     */
    autoBodyPadding?: boolean;
}

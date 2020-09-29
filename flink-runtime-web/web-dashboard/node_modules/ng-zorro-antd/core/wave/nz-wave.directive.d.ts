import { ElementRef, InjectionToken, NgZone, OnDestroy, OnInit } from '@angular/core';
export interface NzWaveConfig {
    disabled?: boolean;
}
export declare const NZ_WAVE_GLOBAL_DEFAULT_CONFIG: NzWaveConfig;
export declare const NZ_WAVE_GLOBAL_CONFIG: InjectionToken<NzWaveConfig>;
export declare function NZ_WAVE_GLOBAL_CONFIG_FACTORY(): NzWaveConfig;
export declare class NzWaveDirective implements OnInit, OnDestroy {
    private ngZone;
    private elementRef;
    private animationType;
    nzWaveExtraNode: boolean;
    private waveRenderer;
    private waveDisabled;
    constructor(ngZone: NgZone, elementRef: ElementRef, config: NzWaveConfig, animationType: string);
    ngOnDestroy(): void;
    ngOnInit(): void;
    renderWaveIfEnabled(): void;
}

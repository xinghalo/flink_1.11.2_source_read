import { EventEmitter, OnChanges, SimpleChanges, TemplateRef } from '@angular/core';
import { NgClassType } from '../core/types/ng-class';
export declare class NzAlertComponent implements OnChanges {
    destroy: boolean;
    iconType: string;
    iconTheme: string;
    private isTypeSet;
    private isShowIconSet;
    nzCloseText: string | TemplateRef<void>;
    nzIconType: NgClassType;
    nzMessage: string | TemplateRef<void>;
    nzDescription: string | TemplateRef<void>;
    nzType: 'success' | 'info' | 'warning' | 'error';
    nzCloseable: boolean;
    nzShowIcon: boolean;
    nzBanner: boolean;
    readonly nzOnClose: EventEmitter<boolean>;
    closeAlert(): void;
    onFadeAnimationDone(): void;
    updateIconClassMap(): void;
    ngOnChanges(changes: SimpleChanges): void;
}

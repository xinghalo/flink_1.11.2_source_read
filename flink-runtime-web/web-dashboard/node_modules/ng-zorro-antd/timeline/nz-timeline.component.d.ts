import { AfterContentInit, ChangeDetectorRef, ElementRef, OnChanges, OnDestroy, QueryList, SimpleChanges, TemplateRef } from '@angular/core';
import { NzTimelineItemComponent } from './nz-timeline-item.component';
export declare type NzTimelineMode = 'left' | 'alternate' | 'right';
export declare class NzTimelineComponent implements AfterContentInit, OnChanges, OnDestroy {
    private cdr;
    timeline: ElementRef<HTMLElement>;
    listOfTimeLine: QueryList<NzTimelineItemComponent>;
    _pendingContent: TemplateRef<void>;
    nzMode: NzTimelineMode;
    nzPending: string | boolean | TemplateRef<void>;
    nzPendingDot: string | TemplateRef<void>;
    nzReverse: boolean;
    isPendingBoolean: boolean;
    private destroy$;
    constructor(cdr: ChangeDetectorRef);
    ngOnChanges(changes: SimpleChanges): void;
    ngAfterContentInit(): void;
    ngOnDestroy(): void;
    private updateChildren;
    private reverseChildTimelineDots;
}

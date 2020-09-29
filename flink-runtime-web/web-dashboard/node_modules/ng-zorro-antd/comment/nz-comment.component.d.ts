import { QueryList, TemplateRef } from '@angular/core';
import { NzCommentActionComponent as CommentAction } from './nz-comment-cells';
export declare class NzCommentComponent {
    nzAuthor: string | TemplateRef<void>;
    nzDatetime: string | TemplateRef<void>;
    actions: QueryList<CommentAction>;
    constructor();
}
